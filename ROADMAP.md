# kickterino Roadmap

Feature ideas borrowed from Chatterino, adapted to Kick.com. Items are grouped
into phases; earlier phases are higher priority and lower effort.

## Status

Already shipped:

- Live read-only chat via Kick Centrifugo WebSocket
- OAuth token based message sending
- Timestamps, user colors, badges, emote placeholders
- System events (bans, subs, gifted subs, pins, rewards, kicks)
- Message deletion handling, message cap, clear chat
- Dark default egui theme

## Phase 1 — Core chat UX (done)

- [x] Pause auto-scroll when the user scrolls up; add "jump to bottom" button
- [x] Auto-reconnect with backoff when the WebSocket drops
- [x] Load recent chat history on connect (Kick chat history endpoint)
- [x] Settings panel: font size, timestamp format (12/24h), show/hide badges
- [x] Copy message / copy username (right-click context menu)
- [x] Highlight own messages and @mentions
- [x] Per-user ignore list (hide messages from a user)
- [x] Keyboard shortcuts: Enter to send, Ctrl+L clear, Ctrl+F search

## Phase 2 — Chatterino feel

- [x] Multiple splits/channels (tabs) with independent connections
- [x] Split resizing and layout persistence (tab layout + settings)
- [x] Search/filter within the current chat
- [x] User card popup on username click (avatar, bio, follow stats)
- [x] Message reply threading (Kick replies_to)
- [x] Emote image rendering (Kick emotes) instead of text placeholders
- [ ] 7TV / BTTV / FFZ emote support (optional, extra requests)

## Phase 3 — Kick-specific features (done)

- [x] Stream info panel: title, category, viewer count, uptime, followers
- [x] Channel points / reward redemption display
- [x] Polls display and (optional) voting
- [x] Raid / host notifications
- [x] Kicks gifting rich display (amount, gift name)
- [x] Pinned message bar

## Phase 4 — Power user / polish (in progress)

- [x] Mod actions from the client (delete using a mod token)
- [ ] Link previews and image previews in chat
- [x] Themes (light/dark) and font size selection
- [x] Always-on-top window
- [ ] Desktop notifications for mentions/keywords
- [ ] Plugin/scripting support (Lua) — long term
- [ ] Localization (Turkish/English)

## Non-goals (for now)

- Voice/video playback
- Multi-account simultaneous login
- Mobile builds

Contributions welcome: pick an unchecked item and open a PR. Prefer `cargo fmt` and `cargo clippy` before submitting.
