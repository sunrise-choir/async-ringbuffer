//! An asynchronous, fixed-capacity single-reader single-writer ring buffer that notifies the reader onces data becomes available, and notifies the writer once new space for data becomes available. This is done via the AsyncRead and AsyncWrite traits.

#![deny(missing_docs)]
#![feature(offset_to)]

extern crate futures;
extern crate tokio_io;

#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
extern crate void;
#[cfg(test)]
extern crate rand;

use std::cmp::min;
use std::io::{Read, Write, Error};
use std::io::ErrorKind::WouldBlock;
use std::ptr::copy_nonoverlapping;
use std::cell::RefCell;
use std::rc::Rc;

use tokio_io::{AsyncRead, AsyncWrite};
use futures::{Poll, Async};
use futures::task::{Task, current};

/// Creates a new RingBuffer with the given capacity, and returns a handle for
/// writing and a handle for reading.
///
/// # Panics
/// Panics if capacity is `0` or greater than `isize::max_value()`.
pub fn ring_buffer(capacity: usize) -> (Writer, Reader) {
    if capacity == 0 || capacity > (isize::max_value() as usize) {
        panic!("Invalid ring buffer capacity.");
    }

    let mut data: Vec<u8> = Vec::with_capacity(capacity);
    let ptr = data.as_mut_slice().as_mut_ptr();

    let rb = Rc::new(RefCell::new(RingBuffer {
                                      data,
                                      read: ptr,
                                      amount: 0,
                                      task: None,
                                      did_shutdown: false,
                                  }));

    (Writer(Rc::clone(&rb)), Reader(rb))
}

struct RingBuffer {
    data: Vec<u8>,
    // reading resumes from this position, this always points into the buffer
    read: *mut u8,
    // amount of valid data
    amount: usize,
    task: Option<Task>,
    did_shutdown: bool,
}

impl RingBuffer {
    fn park(&mut self) -> Error {
        self.task = Some(current());
        return Error::new(WouldBlock, "");
    }

    fn unpark(&mut self) {
        self.task.take().map(|task| task.notify());
    }

    fn write_ptr(&mut self) -> *mut u8 {
        unsafe {
            let start = self.data.as_mut_slice().as_mut_ptr();
            let diff = start
                .offset(self.data.capacity() as isize)
                .offset_to(self.read.offset(self.amount as isize))
                .unwrap();

            if diff < 0 {
                self.read.offset(self.amount as isize)
            } else {
                start.offset(diff)
            }
        }
    }
}

/// Write access to a nonblocking ring buffer with fixed capacity.
///
/// If there is no space in the buffer to write to, the current task is parked
/// and notified once space becomes available.
pub struct Writer(Rc<RefCell<RingBuffer>>);

impl Drop for Writer {
    fn drop(&mut self) {
        self.0.borrow_mut().unpark();
    }
}

/// Nonblocking `Write` implementation.
impl Write for Writer {
    /// Write data to the RingBuffer. The only error this may return is of kind
    /// `WouldBlock`. When this returns a `WouldBlock` error, the current task
    /// is parked and gets notified once more space becomes available in the
    /// buffer.
    ///
    /// This returns only returns `Ok(0)` if either `buf.len() == 0`, `shutdown`
    /// has been called, or if the corresponding Reader has been dropped and no
    /// more data will be read to free up space for new data. If the Writer task
    /// is parked while the Reader is dropped, the task gets notified.
    ///
    /// If a previous call to `read` returned a `WouldBlock` error, the
    /// corresponding `Reader` is unparked if data was written in this `write`
    /// call.
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        let mut rb = self.0.borrow_mut();

        if buf.len() == 0 || rb.did_shutdown {
            return Ok(0);
        }

        let capacity = rb.data.capacity();
        let start = rb.data.as_mut_slice().as_mut_ptr();
        let end = unsafe { start.offset(capacity as isize) }; // end itself is 1 byte outside the buffer

        if rb.amount == capacity {
            if Rc::strong_count(&self.0) == 1 {
                return Ok(0);
            } else {
                return Err(rb.park());
            }
        }

        let buf_ptr = buf.as_ptr();
        let write_total = min(buf.len(), capacity - rb.amount);

        if (unsafe { rb.write_ptr().offset(write_total as isize) } as *const u8) < end {
            // non-wrapping case
            unsafe { copy_nonoverlapping(buf_ptr, rb.write_ptr(), write_total) };

            rb.amount += write_total;
        } else {
            // wrapping case
            let distance_we = rb.write_ptr().offset_to(end).unwrap() as usize;
            let remaining: usize = write_total - distance_we;

            unsafe { copy_nonoverlapping(buf_ptr, rb.write_ptr(), distance_we) };
            unsafe { copy_nonoverlapping(buf_ptr.offset(distance_we as isize), start, remaining) };

            rb.amount += write_total;
        }

        debug_assert!(rb.read >= start);
        debug_assert!(rb.read < end);
        debug_assert!(rb.amount <= capacity);

        rb.unpark();
        return Ok(write_total);
    }

    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl AsyncWrite for Writer {
    /// Once shutdown has been called, the corresponding reader will return
    /// `Ok(0)` on `read` after all buffered data has been read. If the reader
    /// is parked while this is called, it gets notified.
    fn shutdown(&mut self) -> Poll<(), Error> {
        let mut rb = self.0.borrow_mut();

        if !rb.did_shutdown {
            rb.unpark(); // only unpark on first call, makes this method idempotent
        }
        rb.did_shutdown = true;

        Ok(Async::Ready(()))
    }
}

/// Read access to a nonblocking ring buffer with fixed capacity.
///
/// If there is no data in the buffer to read from, the current task is parked
/// and notified once space becomes available.
pub struct Reader(Rc<RefCell<RingBuffer>>);

impl Drop for Reader {
    fn drop(&mut self) {
        self.0.borrow_mut().unpark();
    }
}

/// Nonblocking `Read` implementation.
impl Read for Reader {
    /// Read data from the RingBuffer. The only error this may return is of kind `WouldBlock`.
    /// When this returns a `WouldBlock` error, the current task is parked and
    /// gets notified once more data becomes available the buffer.
    ///
    /// This returns only returns `Ok(0)` if either `buf.len() == 0`, `shutdown`
    /// was called on the writer and all buffered data has been read, or if the
    /// corresponding Writer has been dropped and no new data will become
    /// available. If the Reader task is parked while the Writer is dropped, the
    /// task gets notified.
    ///
    /// If a previous call to `write` returned a `WouldBlock` error, the
    /// corresponding `Writer` is unparked if data was written in this `read`
    /// call.
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        let mut rb = self.0.borrow_mut();

        if buf.len() == 0 {
            return Ok(0);
        }

        let capacity = rb.data.capacity();
        let start = rb.data.as_mut_slice().as_mut_ptr();
        let end = unsafe { start.offset(capacity as isize) }; // end itself is 1 byte outside the buffer

        if rb.amount == 0 {
            if Rc::strong_count(&self.0) == 1 || rb.did_shutdown {
                return Ok(0);
            } else {
                return Err(rb.park());
            }
        }

        let buf_ptr = buf.as_mut_ptr();
        let read_total = min(buf.len(), rb.amount);

        if (unsafe { rb.read.offset(read_total as isize) } as *const u8) < end {
            // non-wrapping case
            unsafe { copy_nonoverlapping(rb.read, buf_ptr, read_total) };

            rb.read = unsafe { rb.read.offset(read_total as isize) };
            rb.amount -= read_total;
        } else {
            // wrapping case
            let distance_re = rb.read.offset_to(end).unwrap() as usize;
            let remaining: usize = read_total - distance_re;

            unsafe { copy_nonoverlapping(rb.read, buf_ptr, distance_re) };
            unsafe { copy_nonoverlapping(start, buf_ptr.offset(distance_re as isize), remaining) };

            rb.read = unsafe { start.offset(remaining as isize) };
            rb.amount -= read_total;
        }

        debug_assert!(rb.read >= start);
        debug_assert!(rb.read < end);
        debug_assert!(rb.amount <= capacity);

        rb.unpark();
        return Ok(read_total);
    }
}

impl AsyncRead for Reader {}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind::WouldBlock;
    use std::cmp::min;

    use quickcheck::{QuickCheck, StdGen, Gen, Arbitrary};
    use futures::{Future, Async};
    use futures::future::poll_fn;
    use void::Void;
    use rand;
    use rand::Rng;

    use super::*;

    #[derive(Clone, Debug)]
    struct Nums {
        items: Vec<usize>,
    }

    impl Arbitrary for Nums {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let size = g.size();
            let items: Vec<usize> = (0..200).map(|_| g.gen_range(0, size)).collect();
            Nums { items }
        }
    }

    struct WriteAll<'b> {
        buf_sizes: Vec<usize>,
        buf: &'b mut Writer,
        data: Vec<u8>,
        offset: usize,
    }

    impl<'b> WriteAll<'b> {
        fn new(buf_sizes: Vec<usize>, buf: &'b mut Writer) -> WriteAll {
            WriteAll {
                buf_sizes,
                buf,
                data: (0u8..255).collect(),
                offset: 0,
            }
        }

        fn step(&mut self) -> Option<usize> {
            let len = self.buf_sizes.pop().unwrap_or(5);
            match self.buf
                      .write(&self.data[self.offset..min(self.offset + len, self.data.len())]) {
                Err(e) => {
                    if e.kind() == WouldBlock {
                        return None;
                    } else {
                        panic!("RingBuffer returned error other than WouldBlock");
                    }
                }
                Ok(written) => {
                    self.offset += written;
                    return Some(written);
                }
            }
        }
    }

    impl<'b> Future for WriteAll<'b> {
        type Item = ();
        type Error = Void;

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            while self.offset < self.data.len() {
                match self.step() {
                    None => return Ok(Async::NotReady),
                    Some(_) => {}
                }
            }
            return Ok(Async::Ready(()));
        }
    }

    struct ReadAll<'b, 'd> {
        buf_sizes: Vec<usize>,
        buf: &'b mut Reader,
        data: &'d mut Vec<u8>,
        offset: usize,
    }

    impl<'b, 'd> ReadAll<'b, 'd> {
        fn new(buf_sizes: Vec<usize>,
               buf: &'b mut Reader,
               data: &'d mut Vec<u8>)
               -> ReadAll<'b, 'd> {
            ReadAll {
                buf_sizes,
                buf,
                data,
                offset: 0,
            }
        }

        fn step(&mut self) -> Option<usize> {
            let len = self.buf_sizes.pop().unwrap_or(5);
            let end = self.data.len();
            match self.buf
                      .read(&mut self.data[self.offset..min(self.offset + len, end)]) {
                Err(e) => {
                    if e.kind() == WouldBlock {
                        return None;
                    } else {
                        panic!("RingBuffer returned error other than WouldBlock");
                    }
                }
                Ok(read) => {
                    self.offset += read;
                    assert!((min(self.offset + len, end) == self.offset) || read != 0); // the ring buffer never returns 0 on read unless it was passed a 0-length buffer
                    return Some(read);
                }
            }
        }
    }

    impl<'b, 'd> Future for ReadAll<'b, 'd> {
        type Item = ();
        type Error = Void;

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            while self.offset < self.data.len() {
                match self.step() {
                    None => return Ok(Async::NotReady),
                    Some(_) => {}
                }
            }
            return Ok(Async::Ready(()));
        }
    }

    // only works when passing buffers of nonzero length to read/write
    struct ReadWriteInterleaved<'rb, 'rd, 'wb> {
        read_all: ReadAll<'rb, 'rd>,
        write_all: WriteAll<'wb>,
        blocked: Blocked,
        done: Done,
    }

    enum Blocked {
        Reader,
        Writer,
        Neither,
    }

    #[derive(Eq, PartialEq)]
    enum Done {
        Reader,
        Writer,
        Neither,
        Both,
    }

    impl<'rb, 'rd, 'wb> ReadWriteInterleaved<'rb, 'rd, 'wb> {
        fn new(r_buf_sizes: Vec<usize>,
               r_buf: &'rb mut Reader,
               r_data: &'rd mut Vec<u8>,
               w_buf_sizes: Vec<usize>,
               w_buf: &'wb mut Writer)
               -> ReadWriteInterleaved<'rb, 'rd, 'wb> {
            ReadWriteInterleaved {
                read_all: ReadAll::new(r_buf_sizes, r_buf, r_data),
                write_all: WriteAll::new(w_buf_sizes, w_buf),
                blocked: Blocked::Neither,
                done: Done::Neither,
            }
        }
    }

    impl<'rb, 'rd, 'wb> Future for ReadWriteInterleaved<'rb, 'rd, 'wb> {
        type Item = ();
        type Error = Void;

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            let mut rng = rand::thread_rng();

            loop {
                match self.done {
                    Done::Neither => {
                        match self.blocked {
                            Blocked::Neither => {
                                if rng.gen() {
                                    match self.write_all.step() {
                                        None => self.blocked = Blocked::Writer,
                                        Some(_) => {
                                            if !(self.write_all.offset <
                                                 self.write_all.data.len()) {
                                                self.done = Done::Writer;
                                            }
                                        }
                                    }
                                } else {
                                    match self.read_all.step() {
                                        None => self.blocked = Blocked::Reader,
                                        Some(_) => {
                                            if !(self.read_all.offset < self.read_all.data.len()) {
                                                self.done = Done::Reader;
                                            }
                                        }
                                    }
                                }
                            }

                            Blocked::Reader => {
                                match self.write_all.step() {
                                    None => self.blocked = Blocked::Writer,
                                    Some(read) => {
                                        if read > 0 {
                                            self.blocked = Blocked::Neither;
                                        }
                                        if !(self.write_all.offset < self.write_all.data.len()) {
                                            self.done = Done::Writer;
                                        }
                                    }
                                }
                            }

                            Blocked::Writer => {
                                match self.read_all.step() {
                                    None => self.blocked = Blocked::Reader,
                                    Some(read) => {
                                        if read > 0 {
                                            self.blocked = Blocked::Neither;
                                        }
                                        if !(self.read_all.offset < self.read_all.data.len()) {
                                            self.done = Done::Reader;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    Done::Reader => {
                        match self.write_all.step() {
                            None => panic!("should never reach this state"),
                            Some(_) => {
                                if !(self.write_all.offset < self.write_all.data.len()) {
                                    self.done = Done::Both;
                                }
                            }
                        }
                    }

                    Done::Writer => {
                        match self.read_all.step() {
                            None => panic!("should never reach this state"),
                            Some(_) => {
                                if !(self.read_all.offset < self.read_all.data.len()) {
                                    self.done = Done::Both;
                                }
                            }
                        }
                    }

                    Done::Both => {
                        return Ok(Async::Ready(()));
                    }
                }
            }
        }
    }

    #[test]
    // Write until 8 bytes have been written, then read until 8 bytes have been
    // read. Repeat until done, then check that correct bytes have been read.
    fn test_separate() {
        let rng = StdGen::new(rand::thread_rng(), 12);
        let mut quickcheck = QuickCheck::new().gen(rng).tests(100);
        quickcheck.quickcheck(separate as fn(Nums, Nums) -> bool);
    }

    fn separate(buf_sizes_write: Nums, buf_sizes_read: Nums) -> bool {
        let (mut writer, mut reader) = ring_buffer(8);
        let mut data: Vec<u8> = (0..255).map(|_| 42).collect();
        {
            let write_all = WriteAll::new(buf_sizes_write.items, &mut writer);
            let read_all = ReadAll::new(buf_sizes_read.items, &mut reader, &mut data);

            let (_, _) = write_all.join(read_all).wait().unwrap();
        }

        for (i, byte) in data.iter().enumerate() {
            if *byte != (i as u8) {
                return false;
            }
        }

        return true;
    }

    #[test]
    // Interleaved reads and writes until done, then check that correct bytes
    // have been read.
    fn test_interleaved() {
        let rng = StdGen::new(rand::thread_rng(), 11);
        let mut quickcheck = QuickCheck::new().gen(rng).tests(1000);
        quickcheck.quickcheck(interleaved as fn(Nums, Nums) -> bool);
    }

    fn interleaved(buf_sizes_write: Nums, buf_sizes_read: Nums) -> bool {
        let (mut writer, mut reader) = ring_buffer(8);
        let mut data: Vec<u8> = (0..255).map(|_| 42).collect();
        {
            let _ = ReadWriteInterleaved::new(buf_sizes_read.items,
                                              &mut reader,
                                              &mut data,
                                              buf_sizes_write.items,
                                              &mut writer)
                    .wait()
                    .unwrap();
        }

        for (i, byte) in data.iter().enumerate() {
            if *byte != (i as u8) {
                return false;
            }
        }

        return true;
    }

    #[test]
    // If the reader has been dropped, writes will return Ok(0) once the buffer
    // is full.
    fn test_dropped_reader() {
        let mut writer;
        {
            let (w, _) = ring_buffer(8);
            writer = w;
        }
        assert_eq!(writer.write(&[0, 1, 2, 3, 4]).unwrap(), 5);
        assert_eq!(writer.write(&[5, 6, 7, 8, 9]).unwrap(), 3);
        assert_eq!(writer.write(&[8, 9]).unwrap(), 0);
        assert_eq!(writer.write(&[8, 9]).unwrap(), 0);
    }

    #[test]
    // If the reader is dropped while the writer is parked, the writer is
    // notified and further writes return Ok(0).
    fn test_dropped_reader_notify() {
        let mut writer = None;
        let mut blocked = false;
        assert_eq!(poll_fn::<(), Void, _>(|| if !blocked {
                                              let (mut w, mut r) = ring_buffer(8);
                                              assert_eq!(w.write(&[0, 1, 2, 3, 4, 5, 6, 7])
                                                             .unwrap(),
                                                         8);
                                              let _ = w.write(&[8, 9]).unwrap_err();
                                              blocked = true;
                                              writer = Some(w);
                                              let _ = r.read(&mut []); // use r so that drop does not get moved to an earlier point
                                              return Ok(Async::NotReady);
                                          } else {
                                              let mut w = writer.take().unwrap();
                                              assert_eq!(w.write(&[8, 9]).unwrap(), 0);
                                              assert_eq!(w.write(&[8, 9]).unwrap(), 0);
                                              return Ok(Async::Ready(()));
                                          })
                           .wait()
                           .unwrap(),
                   ());
    }

    #[test]
    // If the writer has been dropped, reads will return Ok(0) once all data
    // has been read.
    fn test_dropped_writer() {
        let mut reader;
        {
            let (mut w, r) = ring_buffer(8);
            assert_eq!(w.write(&[0, 1, 2, 3, 4, 5, 6, 7]).unwrap(), 8);
            reader = r;
        }
        let mut foo = [0u8; 8];
        assert_eq!(reader.read(&mut foo[0..5]).unwrap(), 5);
        assert_eq!(reader.read(&mut foo[5..8]).unwrap(), 3);
        assert_eq!(reader.read(&mut foo[0..8]).unwrap(), 0);
        assert_eq!(reader.read(&mut foo[0..8]).unwrap(), 0);
    }

    #[test]
    // If the writer is dropped while the reader is parked, the reader is
    // notified and further reads return Ok(0).
    fn test_dropped_writer_notify() {
        let mut reader = None;
        let mut blocked = false;
        assert_eq!(poll_fn::<(), Void, _>(|| if !blocked {
                                              let (mut w, mut r) = ring_buffer(8); // must give writer a name, or dropping optimized to an earlier point
                                              let mut foo = [0u8; 8];

                                              let _ = r.read(&mut foo[0..5]).unwrap_err();
                                              blocked = true;
                                              reader = Some(r);
                                              let _ = w.write(&[]); // use w so that drop does not get moved to an earlier point
                                              return Ok(Async::NotReady);
                                          } else {
                                              let mut r = reader.take().unwrap();
                                              let mut foo = [0u8; 8];

                                              assert_eq!(r.read(&mut foo[0..5]).unwrap(), 0);
                                              assert_eq!(r.read(&mut foo[0..5]).unwrap(), 0);
                                              return Ok(Async::Ready(()));
                                          })
                           .wait()
                           .unwrap(),
                   ());
    }

    #[test]
    // After calling `shutdown` on the writer, reads return Ok(0) once all data has
    // been read.
    fn test_shutdown_writer_reads() {
        let (mut w, mut r) = ring_buffer(8);

        assert_eq!(w.write(&[0, 1, 2, 3, 4, 5]).unwrap(), 6);
        assert_eq!(w.shutdown().unwrap(), Async::Ready(()));

        let mut foo = [0u8; 8];
        assert_eq!(r.read(&mut foo[0..4]).unwrap(), 4);
        assert_eq!(r.read(&mut foo[4..8]).unwrap(), 2);
        assert_eq!(r.read(&mut foo[6..8]).unwrap(), 0);
        assert_eq!(r.read(&mut foo[6..8]).unwrap(), 0);
    }

    #[test]
    // If `shutdown` is called on the writer while the reader is parked, the reader
    // is notified and further reads return Ok(0).
    fn test_shutdown_writer_notify() {
        let (mut w, mut r) = ring_buffer(8);
        let mut foo = [0u8; 8];
        let mut blocked = false;

        assert_eq!(poll_fn::<(), Void, _>(|| if !blocked {
                                              let _ = r.read(&mut foo[0..5]).unwrap_err();
                                              blocked = true;
                                              assert_eq!(w.shutdown().unwrap(), Async::Ready(()));
                                              return Ok(Async::NotReady);
                                          } else {
                                              assert_eq!(r.read(&mut foo[0..5]).unwrap(), 0);
                                              assert_eq!(r.read(&mut foo[0..5]).unwrap(), 0);
                                              return Ok(Async::Ready(()));
                                          })
                           .wait()
                           .unwrap(),
                   ());
    }

    #[test]
    // After calling `shutdown` on the writer, further writes return Ok(0).
    fn test_shutdown_writer_writes() {
        let (mut w, _) = ring_buffer(8);

        assert_eq!(w.write(&[0, 1, 2, 3]).unwrap(), 4);
        assert_eq!(w.shutdown().unwrap(), Async::Ready(()));
        assert_eq!(w.write(&[4, 5, 6, 7]).unwrap(), 0);
        assert_eq!(w.write(&[4, 5, 6, 7]).unwrap(), 0);
    }

}
