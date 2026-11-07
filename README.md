# kickterino

A lightweight Kick.com chat client written in Rust + egui, inspired by Chatterino.

## Build

```sh
cargo build --release
./target/release/kickterino
```

Enter a Kick channel slug (e.g. `xqc`) and press **Connect** to watch live chat.

## Notes

- Uses Kick's realtime WebSocket (Centrifugo protocol) for live chat.
- Requires `curl` on `PATH`; it is used to query Kick's public API for channel and realtime credentials.
- Sending messages is supported by pasting a Kick OAuth access token with `chat:write` scope into the **Access token** field.

## Connecting your Kick account

1. Create an OAuth application in the [Kick Developer Portal](https://dev.kick.com) (or reuse one) and note its **Client ID**.
2. Authorize your account with the `chat:write` scope using OAuth 2.1 with PKCE. Kick returns an **access token**.
3. Run kickterino, enter the channel slug and press **Connect**.
4. Paste the access token into the **Access token** field.
5. Type a message and press **Send**.

> The app never stores your token; it is kept only in memory for the session. Token handling and refresh are your responsibility.

## Acknowledgements

- Inspired by [Chatterino 2](https://github.com/Chatterino/chatterino2) (MIT License).
- Kick API access via the [`kick-api`](https://crates.io/crates/kick-api) Rust crate.