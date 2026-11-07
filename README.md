# kickterino

A lightweight Kick.com chat client written in Rust + egui, inspired by Chatterino.

## Build

```sh
cargo build --release
./target/release/kickterino
```

Enter a Kick channel slug (e.g. `xqc`) and press **Connect** to watch live chat.

## Notes

- Uses Kick's unofficial Pusher WebSocket via `kick-api` (read-only chat view).
- Sending messages requires OAuth authentication and is not implemented yet.