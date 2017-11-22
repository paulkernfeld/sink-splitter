# sink-splitter

An input Sink that routes each item to one of many output sinks.

Insert your output sink before you start sending items for it.

Items are delivered in order. If the output sink corresponding to the current key blocks, then
the `Splitter` will also block.

```rust
extern crate futures;
extern crate sink_splitter;

use futures::{Future, Sink, Stream};
use futures::stream::iter_ok;

fn main() {
   let source = iter_ok::<_, sink_splitter::Error<_, _, _>>(
       vec![("left", 1), ("right", 2), ("right", 3), ("left", 4)],
   );

   let destination_left = Vec::new();
   let destination_right = Vec::new();

   let (splitter, mut controller) = sink_splitter::new();
   controller.insert_sink("left", destination_left).unwrap();
   controller.insert_sink("right", destination_right).unwrap();

   let (_, splitter) = source.forward(splitter).wait().unwrap();
   let sinks = splitter.into_sinks();

   assert_eq!(sinks.get("left").unwrap(), &vec![1, 4]);
   assert_eq!(sinks.get("right").unwrap(), &vec![2, 3]);
}
```

License: MIT/Apache-2.0
