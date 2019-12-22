use crate::{ring_buffer, Reader, Writer};

use core::pin::Pin;
use core::task::Context;
use futures_io::{AsyncRead, AsyncWrite, Result};
use std::task::Poll;

/// An AsyncRead + AsyncWrite stream, which reads from one
/// ringbuffer, and writes to another.
pub struct Duplex {
    r: Reader,
    w: Writer,
}

impl Duplex {
    /// Create a pair of duplex AsyncRead + AsyncWrite streams,
    /// for duplex communication between two entities (eg. client and server).
    pub fn pair(capacity: usize) -> (Duplex, Duplex) {
        let (a_w, a_r) = ring_buffer(capacity);
        let (b_w, b_r) = ring_buffer(capacity);

        (Duplex { r: a_r, w: b_w }, Duplex { r: b_r, w: a_w })
    }

    /// Split duplex AsyncRead + AsyncWrite stream into separate
    /// (AsyncRead, AsyncWrite) halves.
    pub fn split(self) -> (Reader, Writer) {
        let Duplex { r, w } = self;
        (r, w)
    }
}

impl AsyncRead for Duplex {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut self.r).poll_read(cx, buf)
    }
}
impl AsyncWrite for Duplex {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        Pin::new(&mut self.w).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        Pin::new(&mut self.w).poll_flush(cx)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        Pin::new(&mut self.w).poll_close(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn back_and_forth() {
        let (mut client, mut server) = Duplex::pair(1024);
        use futures::executor::block_on;

        block_on(async {
            client.write(&[1, 2, 3, 4, 5]).await.unwrap();
            server.write(&[5, 4, 3, 2, 1]).await.unwrap();
            let mut buf = [0; 5];
            server.read(&mut buf).await.unwrap();
            assert_eq!(&buf, &[1, 2, 3, 4, 5]);
            client.read(&mut buf).await.unwrap();
            assert_eq!(&buf, &[5, 4, 3, 2, 1]);

            server.write(&[6, 7, 8, 9, 10]).await.unwrap();
            client.read(&mut buf).await.unwrap();
            assert_eq!(&buf, &[6, 7, 8, 9, 10]);

            let mut buf = [0; 3];
            let (mut sr, mut sw) = server.split();
            sw.write(&[1, 2, 3]).await.unwrap();
            client.read(&mut buf).await.unwrap();
            assert_eq!(&buf, &[1, 2, 3]);

            client.write(&[3, 2, 1]).await.unwrap();
	    sr.read(&mut buf).await.unwrap();
	    assert_eq!(&buf, &[3, 2, 1]);
        });
    }
}
