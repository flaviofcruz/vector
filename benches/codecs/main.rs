use criterion::criterion_main;

mod character_delimited_bytes;
mod encoder;
mod newline_bytes;

#[cfg(all(feature = "sinks-databricks-zerobus", feature = "codecs-arrow"))]
mod wire_to_arrow_bench;

#[cfg(all(feature = "sinks-databricks-zerobus", feature = "codecs-arrow"))]
criterion_main!(
    character_delimited_bytes::benches,
    newline_bytes::benches,
    encoder::benches,
    wire_to_arrow_bench::benches,
);

#[cfg(not(all(feature = "sinks-databricks-zerobus", feature = "codecs-arrow")))]
criterion_main!(
    character_delimited_bytes::benches,
    newline_bytes::benches,
    encoder::benches,
);
