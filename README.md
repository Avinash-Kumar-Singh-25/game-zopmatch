# zopcloud-game

**ZopMatch** — one neon "Let's Play" memory game. A single Node app, two ways to play (chosen on the lobby screen).

## The three play modes (one app)

**Solo** — one player races the 1:40 clock to clear all 12 pairs; the end screen shows pairs matched and move count.


**A. Player 1 vs Player 2 (same device)** — pass-and-play. Enter both names, then two people take turns on one screen.

**B. Play with a friend (online)** — real-time, room-based, for **2–4 players** on different devices:

- Enter your name and **create a room** → get a 4-letter code (and a shareable invite link).
- Others **join with the code** (or the `?room=CODE` link).
- The host starts. Players **take turns** flipping two tiles; a match scores a point and you go again, a miss passes the turn. Everyone sees the same board update live.

In both modes: a **1:15 (75s) countdown** runs (turns magenta in the final 15s). The match ends when all 12 pairs are matched **or** the clock hits 0 — whoever has the most pairs at that point wins. The end screen shows a **ranked leaderboard with a crown on the winner** (ties handled). Host/players can **Play again**. Tiles are the 4 Zop product marks + 8 cloud/DevOps logos (Kubernetes, Docker, AWS EC2, AWS RDS, Terraform, Git, Nginx, Redis). Animated neon background.

## Architecture

- **Zero dependencies.** The server is a raw WebSocket implementation (RFC 6455 handshake + framing) on Node's built-in `http` module — no `ws`, no Express, no `npm install` needed. Online game state is authoritative on the server; the same-device mode runs entirely in the browser.
- `server.js` serves the client from `public/` and runs the online game rooms in memory.
- Listens on `process.env.PORT` (default 3000).

## Robustness (online mode)

- **Reconnect.** Each player gets a token; the client stores `{code, pid, name}` in `localStorage`. A refresh or dropped connection auto-rejoins the **same seat** (up to 8 retries with backoff). Server holds the seat for a **30s grace window** before dropping it — so a blip no longer breaks the room, and a disconnect is no longer an instant "free win."
- **Restart survival.** Rooms are snapshotted to `rooms.json` (best-effort, debounced) and reloaded on boot, with game timers re-armed. A server redeploy no longer wipes live games — clients auto-rejoin from their stored session. (True multi-instance scaling still needs Redis + sticky sessions; this covers single-instance restarts.)
- **Per-turn idle timer.** If a player sits on their turn for 20s, the server auto-passes to the next player so an AFK opponent can't freeze the match.
- **Abuse limits.** Max 500 rooms, per-connection message rate limit (40 / 2s), 8 KB payload cap, and a 60s sweep that clears idle rooms (30 min TTL). Empty rooms are reaped by the grace timers.
- **Rematch** can be triggered by **any** player (not just the host).

## Analytics

- Counters (`roomsCreated`, `playersJoined`, `gamesStarted`, `gamesFinished`, `reconnects`) plus JSON event logs (`room_created`, `game_started`, `game_finished`, `reconnect`) to stdout — visible in ZopDev logs.
- `GET /stats` returns live JSON (counters + activeRooms + activePlayers). `GET /healthz` returns `ok`.

## Polish & accessibility

- **Sound** (Web Audio, no files): flip / match / mismatch / win, with a **mute toggle** in the header (remembered). **Confetti** on the win screen, **haptics** (`navigator.vibrate`) on match/win, red flash on a mismatch. All respect `prefers-reduced-motion`.
  - Audio unlocks on the first click/keypress (browser autoplay policy). If you hear nothing: click once, check the speaker icon isn't muted, and check system volume.

### Using your own sound files (optional)

The built-in tones are synthesized (free, offline, no download). To use real recorded effects instead, edit `SOUND_FILES` near the top of the `<script>` in `public/index.html`:

```js
var SOUND_FILES={ flip:"/sfx/flip.mp3", match:"/sfx/match.mp3", miss:"/sfx/miss.mp3", win:"/sfx/win.mp3" };
```

Drop the files in `public/sfx/` (served automatically) or paste a `data:` URI. Anything left `""` falls back to the synth tone.

**Free, license-friendly sound sources:**
- **Kenney — Interface / UI Audio** (kenney.nl/assets) — CC0 (no attribution), made for game UI. Best fit.
- **Mixkit** (mixkit.co/free-sound-effects) — free, no attribution.
- **Pixabay** (pixabay.com/sound-effects) — free, no attribution.
- **Freesound** (freesound.org) — huge library; check each sound's CC license.
- **Zapsplat** (zapsplat.com) — free with a free account (attribution on free tier).
- **Accessibility**: every tile carries a short **text label** (K8S, DKR, EC2…) so it's distinguishable without color; cards are real `<button>`s (keyboard-playable) with dynamic `aria-label`s; the turn banner is an `aria-live` region; visible focus rings.
- **Win screen**: crowned leaderboard, a **Share result** button (Web Share API, clipboard fallback), and a **"Built on ZopDev — deploy your own"** CTA. A cleared board plays a celebratory fanfare; a time-out plays the buzzer.
- **How-to** line in the waiting room; game-style fonts (**Orbitron** display, **Rajdhani** body).

## Run locally

```bash
cd zopcloud-game
npm start        # or: node server.js
# open http://localhost:3000
```

## Deploy on ZopDev / ZopCloud

A plain Node app: **push and run `npm start`** (= `node server.js`). No build step, no external services, no database. One process serves both the client and the WebSocket game. Make sure the platform routes the assigned `PORT` and allows WebSocket upgrades (standard on ZopDay app hosting).

## Files

- `server.js` — the one server (static host + WebSocket game rooms)
- `public/index.html` — the one client (lobby, both modes, board, winner)
- `package.json` — `npm start`
- `_archive/solo-index.html` — the earlier standalone single-player build (60s timer + personal-best leaderboard), kept for reference only. Not part of the app.

## Possible next steps

- Fold the old single-player timed mode in as a 3rd lobby option
- Per-turn timer (auto-pass if a player stalls)
- Reconnect / rejoin after a dropped connection
- Persist a global win-count leaderboard (needs a small datastore)
