//! An asynchronous, fixed-capacity single-reader single-writer ring buffer that notifies the reader onces data becomes available, and notifies the writer once new space for data becomes available. This is done via the AsyncRead and AsyncWrite traits.

#![deny(missing_docs)]
#![feature(async_await)]
#![feature(ptr_offset_from)]

extern crate futures_io;

#[cfg(test)]
extern crate futures;

use core::pin::Pin;
use core::task::Context;
use std::cmp::min;
use std::ptr::copy_nonoverlapping;
use std::cell::RefCell;
use std::rc::Rc;

use futures_io::{AsyncRead, AsyncWrite, Result};
use std::task::{Poll, Poll::Ready, Poll::Pending, Waker};

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
                                      waker: None,
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
    waker: Option<Waker>,
    did_shutdown: bool,
}

impl RingBuffer {
    fn park(&mut self, waker: &Waker) {
        self.waker = Some(waker.clone());
    }

    fn wake(&mut self) {
        self.waker.take().map(|w| w.wake());
    }

    fn write_ptr(&mut self) -> *mut u8 {
        unsafe {
            let start = self.data.as_mut_slice().as_mut_ptr();
            let diff = self.read.offset(self.amount as isize)
                .offset_from(start.offset(self.data.capacity() as isize));

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

impl Writer {
    /// Returns true if the writer has been closed, and will therefore no longer
    ///  accept writes.
    pub fn is_closed(&self) -> bool {
        self.0.borrow().did_shutdown
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.0.borrow_mut().wake();
    }
}

impl AsyncWrite for Writer {
    /// Write data to the RingBuffer.
    ///
    /// This only returns `Ok(Ready(0))` if either `buf.len() == 0`, `poll_close` has been called,
    /// or if the corresponding `Reader` has been dropped and no more data will be read to free up
    /// space for new data.
    ///
    /// # Errors
    /// This never emits an error.
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        let mut rb = self.0.borrow_mut();

        if buf.len() == 0 || rb.did_shutdown {
            return Ready(Ok(0));
        }

        let capacity = rb.data.capacity();
        let start = rb.data.as_mut_slice().as_mut_ptr();
        let end = unsafe { start.offset(capacity as isize) }; // end itself is 1 byte outside the buffer

        if rb.amount == capacity {
            if Rc::strong_count(&self.0) == 1 {
                return Ready(Ok(0));
            } else {
                rb.park(cx.waker());
                return Pending;
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
            let distance_we = unsafe { end.offset_from(rb.write_ptr()) as usize };
            let remaining: usize = write_total - distance_we;

            unsafe { copy_nonoverlapping(buf_ptr, rb.write_ptr(), distance_we) };
            unsafe { copy_nonoverlapping(buf_ptr.offset(distance_we as isize), start, remaining) };

            rb.amount += write_total;
        }

        debug_assert!(rb.read >= start);
        debug_assert!(rb.read < end);
        debug_assert!(rb.amount <= capacity);

        rb.wake();
        return Ready(Ok(write_total));
    }

    /// # Errors
    /// This never emits an error.
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<()>> {
        Ready(Ok(()))
    }

    /// Once closing is complete, the corresponding reader will always return `Ok(Ready(0))` on
    /// `poll_read` once all remaining buffered data has been read.
    ///
    /// # Errors
    /// This never emits an error.
    fn poll_close(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<()>> {
        let mut rb = self.0.borrow_mut();

        if !rb.did_shutdown {
            rb.wake(); // only unpark on first call, makes this method idempotent
        }
        rb.did_shutdown = true;

        Ready(Ok(()))
    }
}

/// Read access to a nonblocking ring buffer with fixed capacity.
///
/// If there is no data in the buffer to read from, the current task is parked
/// and notified once data becomes available.
pub struct Reader(Rc<RefCell<RingBuffer>>);

impl Reader {
    /// Returns true if the writer side of the ringbuffer has been closed.
    /// Reads will continue to produce data as long as there are still unread
    /// bytes in the ringbuffer.
    pub fn is_closed(&self) -> bool {
        self.0.borrow().did_shutdown
    }
}


impl Drop for Reader {
    fn drop(&mut self) {
        self.0.borrow_mut().wake();
    }
}

impl AsyncRead for Reader {
    /// Read data from the RingBuffer.
    ///
    /// This only returns `Ok(Ready(0))` if either `buf.len() == 0`, `poll_close`
    /// was called on the corresponding `Writer` and all buffered data has been read, or if the
    /// corresponding `Writer` has been dropped.
    ///
    /// # Errors
    /// This never emits an error.
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        let mut rb = self.0.borrow_mut();

        if buf.len() == 0 {
            return Ready(Ok(0));
        }

        let capacity = rb.data.capacity();
        let start = rb.data.as_mut_slice().as_mut_ptr();
        let end = unsafe { start.offset(capacity as isize) }; // end itself is 1 byte outside the buffer

        if rb.amount == 0 {
            if Rc::strong_count(&self.0) == 1 || rb.did_shutdown {
                return Ready(Ok(0));
            } else {
                rb.park(cx.waker());
                return Pending;
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
            let distance_re = unsafe { end.offset_from(rb.read) as usize };
            let remaining: usize = read_total - distance_re;

            unsafe { copy_nonoverlapping(rb.read, buf_ptr, distance_re) };
            unsafe { copy_nonoverlapping(start, buf_ptr.offset(distance_re as isize), remaining) };

            rb.read = unsafe { start.offset(remaining as isize) };
            rb.amount -= read_total;
        }

        debug_assert!(rb.read >= start);
        debug_assert!(rb.read < end);
        debug_assert!(rb.amount <= capacity);

        rb.wake();
        return Ready(Ok(read_total));
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use futures::io::{AsyncReadExt, AsyncWriteExt};
    use futures::future::join;
    use super::*;

    #[test]
    fn it_works() {
        let (mut writer, mut reader) = ring_buffer(8);
        let data: Vec<u8> = (0..255).collect();
        let write_all = async {
            writer.write_all(&data).await.unwrap();
            writer.close().await.unwrap();
        };

        let mut out: Vec<u8> = Vec::with_capacity(256);
        let read_all = reader.read_to_end(&mut out);

        block_on(async { join(write_all, read_all).await });

        for (i, byte) in out.iter().enumerate() {
            assert_eq!(*byte, i as u8);
        }
    }

    #[test]
    #[should_panic]
    /// Calling `ring_buffer` with capacity 0 panics
    fn panic_on_capacity_0() {
        let _ = ring_buffer(0);
    }

    #[test]
    #[should_panic]
    /// Calling `ring_buffer` with capacity (isize::max_value() as usize) + 1 panics
    fn panic_on_capacity_too_large() {
        let _ = ring_buffer((isize::max_value() as usize) + 1);
    }

    #[test]
    fn close() {
        let (mut writer, mut reader) = ring_buffer(8);
        block_on(async {
            writer.write_all(&[1,2,3,4,5]).await.unwrap();
            assert!(!writer.is_closed());
            assert!(!reader.is_closed());

            writer.close().await.unwrap();

            assert!(writer.is_closed());
            assert!(reader.is_closed());

            let r = writer.write_all(&[6, 7, 8]).await;
            assert!(r.is_err());

            let mut buf = [0; 8];
            let n = reader.read(&mut buf).await.unwrap();
            assert_eq!(n, 5);

            let n = reader.read(&mut buf).await.unwrap();
            assert_eq!(n, 0);
        });
    }
}
