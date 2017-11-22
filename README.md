# Async Ringbuffer

An asynchronous, fixed-capacity single-reader single-writer ring buffer that notifies the reader onces data becomes available, and notifies the writer once new space for data becomes available. This is done via the AsyncRead and AsyncWrite traits.

This crate is to `AsyncRead` and `AsyncWrite` what `futures::unsync::spsc` is to `Stream` and `Sink` (except that `futures::unsync::spsc` doesn't currently exist).

[API documentation](https://docs.rs/async-ringbuffer/)
