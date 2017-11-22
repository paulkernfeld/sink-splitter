#![deny(warnings)]
//! An input Sink that routes each item to one of many output sinks.
//!
//! Insert your output sink before you start sending items for it.
//!
//! Items are delivered in order. If the output sink corresponding to the current key blocks, then
//! the `Splitter` will also block.
//!
//! ```rust
//! extern crate futures;
//! extern crate sink_splitter;
//!
//! use futures::{Future, Sink, Stream};
//! use futures::stream::iter_ok;
//!
//! fn main() {
//!    let source = iter_ok::<_, sink_splitter::Error<_, _, _>>(
//!        vec![("left", 1), ("right", 2), ("right", 3), ("left", 4)],
//!    );
//!
//!    let destination_left = Vec::new();
//!    let destination_right = Vec::new();
//!
//!    let (splitter, mut controller) = sink_splitter::new();
//!    controller.insert_sink("left", destination_left).unwrap();
//!    controller.insert_sink("right", destination_right).unwrap();
//!
//!    let (_, splitter) = source.forward(splitter).wait().unwrap();
//!    let sinks = splitter.into_sinks();
//!
//!    assert_eq!(sinks.get("left").unwrap(), &vec![1, 4]);
//!    assert_eq!(sinks.get("right").unwrap(), &vec![2, 3]);
//! }
//! ```
// We should give the user the ability to remove an output sink. This feature will require a bit of
// work because it is possible that this will require notifying the current task.
extern crate futures;

use Error::*;
use futures::{Async, Poll, Sink, StartSend};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::rc::{Rc, Weak};

#[derive(Debug, PartialEq)]
pub enum Error<K, V, E>
where
    K: Eq + Hash,
{
    OutputSinkDoesNotExist { key: K, value: V },
    OutputSinkStartSend { key: K, error: E },
    OutputSinkPollComplete(Vec<(K, E)>),
    OutputSinkClose(Vec<(K, E)>),
}

#[derive(Debug)]
struct Inner<K: Eq + Hash, S> {
    sinks: HashMap<K, S>,
}

/// This is the input Sink that routes each item to one of many output sinks. This sink takes as
/// input (K, V) pairs where K is the key that determines which output sink to route to and V is the
/// value that is sent to the output sink.
///
#[derive(Debug)]
pub struct Splitter<K: Eq + Hash, S> {
    inner: Rc<RefCell<Inner<K, S>>>,
}

impl<K, V, E, S> Sink for Splitter<K, S>
where
    K: Clone + Eq + Hash,
    S: Sink<SinkItem = V, SinkError = E>,
    E: Debug,
{
    type SinkItem = (K, V);
    type SinkError = Error<K, V, E>;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let (key, value) = item;
        if let Some(sink) = self.inner.borrow_mut().sinks.get_mut(&key) {
            sink.start_send(value)
                .map_err(|error| {
                    OutputSinkStartSend {
                        key: key.clone(),
                        error,
                    }
                })
                .map(|start_send| start_send.map(|v| (key, v)))
        } else {
            Err(OutputSinkDoesNotExist { key, value })
        }
    }

    // This currently bubbles up each error from any output sink. This could be a suboptimal
    // experience if the user expects the splitter to tolerate errors.
    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        let mut ready = true;
        let mut errors: Vec<(K, E)> = Default::default();
        for (key, mut sink) in &mut self.inner.borrow_mut().sinks {
            match sink.poll_complete() {
                Ok(Async::Ready(())) => {
                    // this is good
                }
                Ok(Async::NotReady) => {
                    // the output sink will notify the task
                    ready = false
                }
                Err(error) => {
                    errors.push((key.clone(), error));
                }
            }
        }

        if errors.is_empty() {
            if ready {
                Ok(Async::Ready(()))
            } else {
                Ok(Async::NotReady)
            }
        } else {
            Err(OutputSinkPollComplete(errors))
        }
    }

    // This currently bubbles up each error from any output sink. This could be a suboptimal
    // experience if the user expects the splitter to tolerate errors.
    fn close(&mut self) -> Poll<(), Self::SinkError> {
        let mut ready = true;
        let mut errors: Vec<(K, E)> = Vec::default();
        for (key, mut sink) in &mut self.inner.borrow_mut().sinks {
            match sink.close() {
                Ok(Async::Ready(())) => {
                    // this is good
                }
                Ok(Async::NotReady) => {
                    // the output sink will notify the task
                    ready = false
                }
                Err(error) => errors.push((key.clone(), error)),
            }
        }

        if errors.is_empty() {
            if ready {
                Ok(Async::Ready(()))
            } else {
                Ok(Async::NotReady)
            }
        } else {
            Err(OutputSinkClose(errors))
        }
    }
}

impl<K, V, E, S> Splitter<K, S>
where
    K: Clone + Eq + Hash,
    S: Sink<SinkItem = V, SinkError = E>,
    E: Debug,
{
    /// Consume this Splitter and return its map of output sinks. Note that this function does not
    /// close any of the sinks.
    pub fn into_sinks(self) -> HashMap<K, S> {
        if let Ok(ref_cell) = Rc::try_unwrap(self.inner) {
            ref_cell.into_inner().sinks
        } else {
            unreachable!("There is only ever one strong reference to an Inner (from its Splitter)")
        }
    }
}

/// This error will occur when trying to make changes after the Splitter has already been dropped.
#[derive(Debug)]
pub struct SplitterHasBeenDropped {}

/// This struct is used to manipulate the collection of output sinks.
#[derive(Debug)]
pub struct Controller<K: Eq + Hash, S> {
    inner: Weak<RefCell<Inner<K, S>>>,
}

impl<K, V, E, S> Controller<K, S>
where
    K: Eq + Hash,
    S: Sink<SinkItem = V, SinkError = E>,
    E: Debug,
{
    /// Insert an output sink into this `Splitter`. The output sink will receive all items with
    /// this key.
    ///
    /// This method panics if you try to insert a Sink for a key that already has one.
    pub fn insert_sink(&mut self, key: K, sink: S) -> Result<(), SplitterHasBeenDropped> {
        // We don't need to wake up a task because:
        // 1. We know that there was no previous Sink for this key (otherwise we panic)
        // 2. Any start_send for this key will therefore have failed
        if let Some(rc) = self.inner.upgrade() {
            assert!(
                rc.borrow_mut().sinks.insert(key, sink).is_none(),
                "There is already a sink for this key"
            );
            Ok(())
        } else {
            Err(SplitterHasBeenDropped {})
        }
    }
}

/// Create a new connected Splitter and Controller pair.
pub fn new<K, V, E, S>() -> (Splitter<K, S>, Controller<K, S>)
where
    K: Eq + Hash,
    S: Sink<SinkItem = V, SinkError = E>,
    E: Debug,
{
    let inner = Rc::new(RefCell::new(Inner { sinks: HashMap::default() }));
    let weak = Rc::downgrade(&inner);
    (Splitter { inner }, Controller { inner: weak })
}

#[cfg(test)]
mod test {
    extern crate error_test_sinks;

    use super::*;
    use futures::{Future, Sink};

    use self::error_test_sinks::{close_error_sink, poll_complete_error_sink, start_send_error_sink};

    #[test]
    fn test_error_output_sink_does_not_exist() {
        let (splitter, _controller) = new::<_, _, (), Vec<_>>();
        match Sink::send(splitter, (0, 1)).wait().unwrap_err() {
            Error::OutputSinkDoesNotExist { key: 0, value: 1 } => (),
            _ => assert!(false),
        }
    }

    #[test]
    fn test_error_output_sink_start_send() {
        let (splitter, mut controller) = new();
        let key = 0;
        let value = 1;
        let destination = start_send_error_sink::StartSendErrSink::default();
        controller.insert_sink(key, destination).unwrap();
        let err = splitter.send((key, value)).wait().unwrap_err();
        assert_eq!(
            err,
            Error::OutputSinkStartSend {
                key,
                error: start_send_error_sink::Error(value),
            }
        );
    }

    #[test]
    fn test_error_output_sink_poll_complete() {
        let (splitter, mut controller) = new();
        let key = 0;
        let value = 1;
        let destination = poll_complete_error_sink::PollCompleteErrSink::default();
        controller.insert_sink(key, destination).unwrap();
        let err = splitter.send((key, value)).wait().unwrap_err();
        assert_eq!(
            err,
            Error::OutputSinkPollComplete(vec![(key, poll_complete_error_sink::Error())])
        );
    }

    #[test]
    fn test_error_output_sink_close() {
        let (mut splitter, mut controller) = new::<_, i8, _, _>();
        let key = 0;
        let destination = close_error_sink::CloseErrSink::default();
        controller.insert_sink(key, destination).unwrap();
        assert_eq!(
            splitter.close().unwrap_err(),
            Error::OutputSinkClose(vec![(key, close_error_sink::Error())])
        );
    }

    #[test]
    fn basic() {
        let (splitter, mut controller) = new();
        let key = 0;
        let value = 1;
        let destination = vec![];
        controller.insert_sink(key, destination).unwrap();
        let sinks = splitter.send((key, value)).wait().unwrap().into_sinks();
        assert_eq!(sinks.get(&key).unwrap(), &vec![1]);
    }
}
