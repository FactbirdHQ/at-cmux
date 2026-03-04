# at-cmux

A `no_std` implementation of the 3GPP TS 27.010 CMUX (GSM multiplexer) protocol for embedded systems.

This crate allows you to multiplex multiple logical serial channels over a single physical serial port, commonly used with GSM/LTE modems to run AT commands and data connections simultaneously.

## Features

- `no_std` compatible - runs on bare metal embedded systems
- Async/await support via `embedded-io-async`
- Works with any async executor (Embassy, RTIC, etc.)
- Optional logging via `defmt` or `log` features

## Usage

```rust
use at_cmux::Mux;

// Create a multiplexer with 2 channels, 256-byte buffers
let mut mux: Mux<2, 256> = Mux::new();

// Start the multiplexer
let (mut runner, channels) = mux.start();

// Spawn the runner in a background task
// runner.run(serial_rx, serial_tx, 128).await;

// Use channels for communication
let [ch0, ch1] = channels;
// ch0 can be used for AT commands
// ch1 can be used for data (PPP, etc.)
```

## Interoperability

This crate can run on any async executor.

It supports any serial port implementing [`embedded-io-async`](https://crates.io/crates/embedded-io-async).

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
