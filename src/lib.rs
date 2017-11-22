//! An asynchronous, fixed-capacity single-reader single-writer ring buffer that notifies the reader onces data becomes available, and notifies the writer once new space for data becomes available. This is done via the AsyncRead and AsyncWrite traits.

#![deny(missing_docs)]
#![feature(offset_to)]

extern crate futures;
extern crate tokio_io;

#[cfg(test)]
extern crate partial_io;
#[cfg(test)]
#[macro_use]
extern crate quickcheck;

use std::cmp::min;
use std::io::{Read, Write, Error};
use std::io::ErrorKind::WouldBlock;
use std::ptr::copy_nonoverlapping;

use tokio_io::{AsyncRead, AsyncWrite};
use futures::{Poll, Async};
use futures::task::{Task, current};

/// A nonblocking ring buffer with fixed capacity.
///
/// If there is no data available to read or no space in the buffer to write,
/// the current task is parked and notified once data/space becomes available.
/// Since the capacity of the buffer must be nonzero, a reader and a writer task
/// will never be parked at the same time (either there is space for data, or
/// there is data to read).
///
/// The capacity must be greater than `0` and less than `isize::max_value()`
pub struct RingBuffer {
    data: Vec<u8>,
    // reading resumes from this position, this always points into the buffer
    read: *const u8,
    // writing resumes at this position in the buffer, this always points into the buffer
    write: *mut u8,
    task: Option<Task>,
}

impl RingBuffer {
    /// Creates a new RingBuffer with the given capacity.
    ///
    /// # Panics
    /// Panics if capacity is `0` or greater than `isize::max_value()`.
    pub fn new(capacity: usize) -> RingBuffer {
        if capacity == 0 || capacity > (isize::max_value() as usize) {
            panic!("Invalid ring buffer capacity.");
        }

        let mut data: Vec<u8> = Vec::with_capacity(capacity);
        let ptr = data.as_mut_slice().as_mut_ptr();

        RingBuffer {
            data,
            read: ptr,
            write: ptr,
            task: None,
        }
    }

    fn park(&mut self) -> Error {
        self.task = Some(current());
        return Error::new(WouldBlock, "");
    }

    fn unpark(&mut self) {
        self.task.take().map(|task| task.notify());
    }
}

/// Nonblocking `Read` implementation.
impl Read for RingBuffer {
    /// Read data from the RingBuffer. This returns `Ok(0)` if and only if
    /// `buf.len() == 0`. The only error this may return is of kind `WouldBlock`.
    /// When this returns a `WouldBlock` error, the current task is parked and
    /// gets notified once more data becomes available the buffer.
    ///
    /// If a previous call to `write` returned a `WouldBlock` error, the
    /// corresponding task is unparked.
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        if buf.len() == 0 {
            return Ok(0);
        }

        let capacity = self.data.capacity();
        let start = self.data.as_mut_slice().as_mut_ptr();
        let end = unsafe { start.offset(capacity as isize) }; // end itself is 1 byte outside the buffer
        let distance_rw = self.write.offset_to(self.read).unwrap() as usize % capacity;

        if distance_rw == 0 {
            return Err(self.park());
        }

        let buf_ptr = buf.as_mut_ptr();
        let read_total = min(buf.len(), distance_rw);

        if (unsafe { self.read.offset(read_total as isize) } as *const u8) < end {
            // non-wrapping case
            unsafe { copy_nonoverlapping(self.read, buf_ptr, read_total) };

            self.read = unsafe { self.read.offset(read_total as isize) };
        } else {
            // wrapping case
            let distance_re = self.read.offset_to(end).unwrap() as usize;
            let remaining = read_total - distance_re;

            unsafe { copy_nonoverlapping(self.read, buf_ptr, distance_re) };
            unsafe { copy_nonoverlapping(start, buf_ptr.offset(distance_re as isize), remaining) };

            self.read = unsafe { self.read.offset(remaining as isize) };
        }

        // invariants: read and write always point to valid memory
        debug_assert!(self.read >= start);
        debug_assert!(self.read < end);
        debug_assert!((self.write as *const u8) >= start);
        debug_assert!((self.write as *const u8) < end);

        self.unpark();
        return Ok(read_total);
    }
}

/// Nonblocking `Write` implementation.
impl Write for RingBuffer {
    /// Write data to the RingBuffer. This returns `Ok(0)` if and only if
    /// `buf.len() == 0`. The only error this may return is of kind `WouldBlock`.
    /// When this returns a `WouldBlock` error, the current task is parked and
    /// gets notified once more space becomes available in the buffer.
    ///
    /// If a previous call to `read` returned a `WouldBlock` error, the
    /// corresponding task is unparked.
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        if buf.len() == 0 {
            return Ok(0);
        }

        let capacity = self.data.capacity();
        let start = self.data.as_mut_slice().as_mut_ptr();
        let end = unsafe { start.offset(capacity as isize) }; // end itself is 1 byte outside the buffer
        let distance_wr = self.write.offset_to(self.read).unwrap() as usize % capacity;

        if distance_wr == 1 {
            return Err(self.park());
        }

        let buf_ptr = buf.as_ptr();
        let write_total = min(buf.len(), distance_wr);

        if (unsafe { self.write.offset(write_total as isize) } as *const u8) < end {
            unsafe { copy_nonoverlapping(buf_ptr, self.write, write_total) };

            self.write = unsafe { self.write.offset(write_total as isize) };
        } else {
            let distance_we = self.write.offset_to(end).unwrap() as usize;
            let remaining = write_total - distance_we;

            unsafe { copy_nonoverlapping(buf_ptr, self.write, distance_we) };
            unsafe { copy_nonoverlapping(buf_ptr.offset(distance_we as isize), start, remaining) };

            self.write = unsafe { start.offset(remaining as isize) };
        }

        // invariants: read and write always point to valid memory
        debug_assert!(self.read >= start);
        debug_assert!(self.read < end);
        debug_assert!((self.write as *const u8) >= start);
        debug_assert!((self.write as *const u8) < end);

        self.unpark();
        return Ok(write_total);
    }

    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl AsyncRead for RingBuffer {}

impl AsyncWrite for RingBuffer {
    fn shutdown(&mut self) -> Poll<(), Error> {
        Ok(Async::Ready(()))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
