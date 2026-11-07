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

## Phase 1 — Core chat UX (next)

- [ ] Pause auto-scroll when the user scrolls up; add "jump to bottom" button
- [ ] Auto-reconnect with backoff when the WebSocket drops
- [ ] Load recent chat history on connect (Kick chat history endpoint)
- [ ] Settings panel: font size, timestamp format (12/24h), show/hide badges
- [ ] Copy message / copy username (right-click context menu)
- [ ] Highlight own messages and @mentions
- [ ] Per-user ignore list (hide messages from a user)
- [ ] Keyboard shortcuts: Enter to send, Ctrl+L clear, Ctrl+F search

## Phase 2 — Chatterino feel

- [ ] Multiple splits/channels (tabs) with independent connections
- [ ] Split resizing and layout persistence
- [ ] Search/filter within the current chat
- [ ] User card popup on username click (avatar, bio, follow stats)
- [ ] Message reply threading (Kick replies_to)
- [ ] Emote image rendering (Kick emotes) instead of text placeholders
- [ ] 7TV / BTTV / FFZ emote support (optional, extra requests)

## Phase 3 — Kick-specific features

- [ ] Stream info panel: title, category, viewer count, uptime, followers
- [ ] Channel points / reward redemption display
- [ ] Polls display and (optional) voting
- [ ] Raid / host notifications
- [ ] Kicks gifting rich display (amount, gift name)
- [ ] Pinned message bar

## Phase 4 — Power user / polish

- [ ] Mod actions from the client (ban, timeout, delete) using a mod token
- [ ] Link previews and image previews in chat
- [ ] Themes (light/dark/custom) and font selection
- [ ] Always-on-top window, tray icon, global hotkeys
- [ ] Desktop notifications for mentions/keywords
- [ ] Plugin/scripting support (Lua) — long term
- [ ] Localization (Turkish/English)

## Non-goals (for now)

- Voice/video playback
- Multi-account simultaneous login
- Mobile builds

Contributions welcome: pick an unchecked item and open a PR. Prefer `cargo fmt` and `cargo clippy` before submitting.
