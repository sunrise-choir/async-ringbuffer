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
use std::rc::{Rc, Weak};

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
                                  }));

    (Writer(Rc::downgrade(&rb)), Reader(rb))
}

struct RingBuffer {
    data: Vec<u8>,
    // reading resumes from this position, this always points into the buffer
    read: *mut u8,
    // amount of valid data
    amount: usize,
    task: Option<Task>,
}

impl RingBuffer {
    fn park(&mut self) -> Error {
        self.task = Some(current());
        return Error::new(WouldBlock, "");
    }

    fn unpark(&mut self) {
        self.task.take().map(|task| task.notify());
    }

    // TODO when to use this?
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

impl Read for RingBuffer {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        println!("called read");
        if buf.len() == 0 {
            println!("read received empty buffer");
            return Ok(0);
        }

        let capacity = self.data.capacity();
        let start = self.data.as_mut_slice().as_mut_ptr();
        let end = unsafe { start.offset(capacity as isize) }; // end itself is 1 byte outside the buffer

        if self.amount == 0 {
            println!("parked in read");
            return Err(self.park());
        }

        let buf_ptr = buf.as_mut_ptr();
        let read_total = min(buf.len(), self.amount);

        println!("read status: capacity = {}, start = {:?}, end = {:?}, amount = {}, read_total = {}",
                 capacity,
                 start,
                 end,
                 self.amount,
                 read_total);
        println!("read: read = {:?}, amount = {:?}", self.read, self.amount);


        if (unsafe { self.read.offset(read_total as isize) } as *const u8) < end {
            // non-wrapping case
            println!("read: nonwrapping");
            unsafe { copy_nonoverlapping(self.read, buf_ptr, read_total) };

            self.read = unsafe { self.read.offset(read_total as isize) };
            self.amount -= read_total;
        } else {
            // wrapping case
            println!("read: wrapping");
            let distance_re = self.read.offset_to(end).unwrap() as usize;
            let remaining: usize = read_total - distance_re;

            unsafe { copy_nonoverlapping(self.read, buf_ptr, distance_re) };
            unsafe { copy_nonoverlapping(start, buf_ptr.offset(distance_re as isize), remaining) };

            self.read = unsafe { start.offset(remaining as isize) };
            self.amount -= read_total;
        }
        println!("read new status: read = {:?}, amount = {:?}",
                 self.read,
                 self.amount);

        debug_assert!(self.read >= start);
        debug_assert!(self.read < end);
        debug_assert!(self.amount <= capacity);

        self.unpark();
        return Ok(read_total);
    }
}

impl Write for RingBuffer {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        println!("called write");
        if buf.len() == 0 {
            println!("write received empty buffer");
            return Ok(0);
        }

        let capacity = self.data.capacity();
        let start = self.data.as_mut_slice().as_mut_ptr();
        let end = unsafe { start.offset(capacity as isize) }; // end itself is 1 byte outside the buffer

        if self.amount == capacity {
            println!("parked in write");
            return Err(self.park());
        }

        let buf_ptr = buf.as_ptr();
        let write_total = min(buf.len(), capacity - self.amount);

        println!("write status: capacity = {}, start = {:?}, end = {:?}, amount = {}, write_total = {}",
                 capacity,
                 start,
                 end,
                 self.amount,
                 write_total);
        println!("write: read = {:?}, amount = {:?}", self.read, self.amount);

        if (unsafe { self.write_ptr().offset(write_total as isize) } as *const u8) < end {
            unsafe { copy_nonoverlapping(buf_ptr, self.write_ptr(), write_total) };

            self.amount += write_total;
        } else {
            let distance_we = self.write_ptr().offset_to(end).unwrap() as usize;
            let remaining: usize = write_total - distance_we;

            unsafe { copy_nonoverlapping(buf_ptr, self.write_ptr(), distance_we) };
            unsafe { copy_nonoverlapping(buf_ptr.offset(distance_we as isize), start, remaining) };

            self.amount += write_total;
        }
        println!("write new status: read = {:?}, amount = {:?}",
                 self.read,
                 self.amount);

        debug_assert!(self.read >= start);
        debug_assert!(self.read < end);
        debug_assert!(self.amount <= capacity);

        self.unpark();
        return Ok(write_total);
    }

    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

/// Write access to a nonblocking ring buffer with fixed capacity.
///
/// If there is no space in the buffer to write to, the current task is parked
/// and notified once space becomes available.
pub struct Writer(Weak<RefCell<RingBuffer>>);

/// Nonblocking `Write` implementation.
impl Write for Writer {
    /// Write data to the RingBuffer. This returns `Ok(0)` if and only if
    /// `buf.len() == 0`. The only error this may return is of kind `WouldBlock`.
    /// When this returns a `WouldBlock` error, the current task is parked and
    /// gets notified once more space becomes available in the buffer.
    ///
    /// If a previous call to `read` returned a `WouldBlock` error, the
    /// corresponding `Reader` is unparked if data was written in this `write`
    /// call.
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        match self.0.upgrade() {
            Some(rb) => rb.borrow_mut().write(buf),
            None => unreachable!(),
        }
    }

    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl AsyncWrite for Writer {
    fn shutdown(&mut self) -> Poll<(), Error> {
        Ok(Async::Ready(()))
    }
}

/// Read access to a nonblocking ring buffer with fixed capacity.
///
/// If there is no data in the buffer to read from, the current task is parked
/// and notified once space becomes available.
pub struct Reader(Rc<RefCell<RingBuffer>>);

/// Nonblocking `Read` implementation.
impl Read for Reader {
    /// Read data from the RingBuffer. This returns `Ok(0)` if and only if
    /// `buf.len() == 0`. The only error this may return is of kind `WouldBlock`.
    /// When this returns a `WouldBlock` error, the current task is parked and
    /// gets notified once more data becomes available the buffer.
    ///
    /// If a previous call to `write` returned a `WouldBlock` error, the
    /// corresponding `Writer` is unparked if data was written in this `read`
    /// call.
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        self.0.borrow_mut().read(buf)
    }
}

impl AsyncRead for Reader {}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind::WouldBlock;
    use std::cmp::min;

    use quickcheck::{QuickCheck, StdGen, Gen, Arbitrary};
    use futures::{Future, Async};
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
            println!("");
            println!("WriteAll poll with {} buffer", len);
            match self.buf
                      .write(&self.data[self.offset..min(self.offset + len, self.data.len())]) {
                Err(e) => {
                    if e.kind() == WouldBlock {
                        println!("WriteAll returns NotReady");
                        return None;
                    } else {
                        panic!("RingBuffer returned error other than WouldBlock");
                    }
                }
                Ok(written) => {
                    println!("WriteAll wrote {}, new offset is {}",
                             written,
                             self.offset + written);
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
            println!("");
            println!("ReadAll poll with {} buffer", len);
            match self.buf
                      .read(&mut self.data[self.offset..min(self.offset + len, end)]) {
                Err(e) => {
                    if e.kind() == WouldBlock {
                        println!("ReadAll returns NotReady");
                        return None;
                    } else {
                        panic!("RingBuffer returned error other than WouldBlock");
                    }
                }
                Ok(read) => {
                    println!("ReadAll read {}, new offset is {}",
                             read,
                             self.offset + read);
                    self.offset += read;
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
}
