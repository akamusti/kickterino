# kickterino

A lightweight Kick.com chat client written in Rust + egui, inspired by Chatterino.

## Build

```sh
cargo build --release
./target/release/kickterino
```

Enter a Kick channel slug (e.g. `xqc`) and press **Connect** to watch live chat.

## Notes

- Uses Kick's unofficial Pusher WebSocket via `kick-api` for live chat.
- Sending messages is supported by pasting a Kick OAuth access token with `chat:write` scope into the **Access token** field.