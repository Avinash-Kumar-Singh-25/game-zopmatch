/*
 * ZopMatch - real-time multiplayer memory game.
 * Zero dependencies: raw WebSocket (RFC 6455) on top of Node's http server.
 * Serves the client from ./public and hosts turn-based game rooms.
 *
 * Robustness: reconnect tokens + grace period, room TTL/idle cleanup,
 * max-rooms cap, per-connection message throttle, payload cap.
 * Analytics: counters + JSON event logs + GET /stats.
 */
"use strict";

const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PORT = process.env.PORT || 3000;
const PUBLIC = path.join(__dirname, "public");
const MIME = {
  ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8", ".svg": "image/svg+xml", ".ico": "image/x-icon",
  ".png": "image/png", ".json": "application/json",
  ".wav": "audio/wav", ".mp3": "audio/mpeg", ".ogg": "audio/ogg"
};

/* ---------------- limits & timing ---------------- */
const PAIRS = 12, N = PAIRS * 2, POOL = 16;   // 12 pairs per game, drawn from 16 logo types
const RESOLVE_MS = 900;
const GAME_MS = 100000;             // 1 min 40 sec per match
const GRACE_MS = 30000;             // reconnect window before a player is dropped
const ROOM_IDLE_MS = 30 * 60 * 1000;// idle rooms swept after 30 min
const MAX_ROOMS = 500;              // hard cap to bound memory
const MAX_PAYLOAD = 8 * 1024;       // per-message byte cap
const MSG_WINDOW_MS = 2000, MSG_MAX = 40; // per-connection rate limit
const TURN_MS = 20000;              // per-turn idle limit before auto-pass
const PERSIST_FILE = path.join(__dirname, "rooms.json"); // best-effort restart survival

/* ---------------- analytics ---------------- */
const stats = { roomsCreated: 0, playersJoined: 0, gamesStarted: 0, gamesFinished: 0, reconnects: 0, startedAt: new Date().toISOString() };
function track(evt, extra) { try { console.log(JSON.stringify(Object.assign({ evt, ts: new Date().toISOString() }, extra || {}))); } catch (e) {} }

/* ---------------- static + stats server ---------------- */
const server = http.createServer((req, res) => {
  const urlPath = decodeURIComponent((req.url || "/").split("?")[0]);
  if (urlPath === "/stats") {
    res.writeHead(200, { "Content-Type": "application/json" });
    return res.end(JSON.stringify(Object.assign({}, stats, { activeRooms: rooms.size, activePlayers: activePlayers() }), null, 2));
  }
  if (urlPath === "/healthz") { res.writeHead(200); return res.end("ok"); }
  let file = path.join(PUBLIC, path.normalize(urlPath === "/" ? "/index.html" : urlPath));
  if (!file.startsWith(PUBLIC)) { res.writeHead(403); return res.end("forbidden"); }
  fs.readFile(file, (err, data) => {
    if (err) {
      fs.readFile(path.join(PUBLIC, "index.html"), (e2, d2) => {
        if (e2) { res.writeHead(404); return res.end("not found"); }
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" }); res.end(d2);
      });
      return;
    }
    res.writeHead(200, { "Content-Type": MIME[path.extname(file)] || "application/octet-stream" });
    res.end(data);
  });
});

/* ---------------- websocket handshake ---------------- */
const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if (!key) return socket.destroy();
  const accept = crypto.createHash("sha1").update(key + GUID).digest("base64");
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n");
  handleConnection(socket);
});

/* ---------------- frame codec ---------------- */
function decodeFrame(buf) {
  if (buf.length < 2) return null;
  const b1 = buf[1];
  const opcode = buf[0] & 0x0f;
  const masked = (b1 & 0x80) !== 0;
  let len = b1 & 0x7f, offset = 2;
  if (len === 126) { if (buf.length < 4) return null; len = buf.readUInt16BE(2); offset = 4; }
  else if (len === 127) { if (buf.length < 10) return null; len = Number(buf.readBigUInt64BE(2)); offset = 10; }
  if (len > MAX_PAYLOAD) return { tooBig: true };
  let maskKey;
  if (masked) { if (buf.length < offset + 4) return null; maskKey = buf.slice(offset, offset + 4); offset += 4; }
  if (buf.length < offset + len) return null;
  let payload = buf.slice(offset, offset + len);
  if (masked) { const out = Buffer.alloc(len); for (let i = 0; i < len; i++) out[i] = payload[i] ^ maskKey[i & 3]; payload = out; }
  return { opcode, payload, rest: buf.slice(offset + len) };
}
function encodeFrame(payload, opcode) {
  const len = payload.length; let header;
  if (len < 126) header = Buffer.from([0x80 | opcode, len]);
  else if (len < 65536) { header = Buffer.alloc(4); header[0] = 0x80 | opcode; header[1] = 126; header.writeUInt16BE(len, 2); }
  else { header = Buffer.alloc(10); header[0] = 0x80 | opcode; header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([header, payload]);
}

/* ---------------- connections ---------------- */
function handleConnection(socket) {
  const conn = { socket, room: null, pid: null, msgTimes: [] };
  conn.send = (obj) => { try { socket.write(encodeFrame(Buffer.from(JSON.stringify(obj)), 0x1)); } catch (e) {} };
  let buf = Buffer.alloc(0);
  socket.on("data", (chunk) => {
    buf = Buffer.concat([buf, chunk]);
    if (buf.length > MAX_PAYLOAD * 4) { socket.destroy(); return; }   // runaway buffer
    let frame;
    while ((frame = decodeFrame(buf))) {
      if (frame.tooBig) { socket.destroy(); return; }
      buf = frame.rest;
      if (frame.opcode === 0x8) { socket.end(); return; }
      else if (frame.opcode === 0x9) socket.write(encodeFrame(frame.payload, 0xA));
      else if (frame.opcode === 0x1) {
        // rate limit
        const now = Date.now();
        conn.msgTimes = conn.msgTimes.filter(t => now - t < MSG_WINDOW_MS);
        conn.msgTimes.push(now);
        if (conn.msgTimes.length > MSG_MAX) continue;
        let msg; try { msg = JSON.parse(frame.payload.toString("utf8")); } catch (e) { continue; }
        onMessage(conn, msg);
      }
    }
  });
  socket.on("close", () => onDisconnect(conn));
  socket.on("error", () => onDisconnect(conn));
}

/* ---------------- game state ---------------- */
const rooms = new Map();
function token() { return crypto.randomUUID(); }
function activePlayers() { let n = 0; rooms.forEach(r => r.players.forEach(p => { if (p.connected) n++; })); return n; }
function code4() {
  const a = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"; let c = "";
  for (let i = 0; i < 4; i++) c += a[Math.floor(Math.random() * a.length)];
  return c;
}
function newDeck() {
  // pick PAIRS distinct logo types out of POOL
  const ids = []; for (let i = 0; i < POOL; i++) ids.push(i);
  for (let i = ids.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); const t = ids[i]; ids[i] = ids[j]; ids[j] = t; }
  const chosen = ids.slice(0, PAIRS);
  const d = []; chosen.forEach(f => { d.push(f); d.push(f); });
  for (let i = d.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); const t = d[i]; d[i] = d[j]; d[j] = t; }
  return d;
}
function makeRoom(hostPid) {
  let code; do { code = code4(); } while (rooms.has(code));
  const room = {
    code, hostId: hostPid, players: [], deck: newDeck(),
    matched: new Array(N).fill(false), up: [], turn: 0,
    started: false, over: false, busy: false, winner: null,
    deadline: null, timer: null, turnTimer: null, turnDeadline: null, lastActivity: Date.now()
  };
  rooms.set(code, room);
  return room;
}
function playerView(room) { return room.players.map(p => ({ id: p.id, name: p.name, score: p.score, connected: p.connected, avatar: p.avatar || 0 })); }
function clampAvatar(a) { a = parseInt(a, 10); return (a >= 0 && a <= 3) ? a : 0; }
function faceView(room) {
  const f = new Array(N).fill(null);
  for (let i = 0; i < N; i++) if (room.over || room.matched[i] || room.up.indexOf(i) >= 0) f[i] = room.deck[i];
  return f;
}
function broadcast(room) {
  const state = {
    type: "state", code: room.code, hostId: room.hostId, players: playerView(room),
    turn: room.turn, started: room.started, over: room.over, busy: room.busy,
    up: room.up.slice(), matched: room.matched.slice(), faces: faceView(room),
    winner: room.winner, deadline: room.deadline, turnDeadline: room.turnDeadline
  };
  room.players.forEach(p => { if (p.connected && p.conn) p.conn.send(state); });
  markDirty();
}
function computeWinner(room) {
  let max = -1; room.players.forEach(p => { if (p.score > max) max = p.score; });
  const top = room.players.filter(p => p.score === max);
  return { names: top.map(p => p.name), score: max, tie: top.length > 1 };
}
function startTimer(room) {
  if (room.timer) clearTimeout(room.timer);
  room.deadline = Date.now() + GAME_MS;
  room.timer = setTimeout(() => { if (rooms.has(room.code) && room.started && !room.over) endGame(room, "time"); }, GAME_MS);
}
function endGame(room, reason) {
  if (room.over) return;
  room.over = true; room.up = []; room.busy = false;
  room.winner = computeWinner(room);
  if (room.timer) { clearTimeout(room.timer); room.timer = null; }
  clearTurnTimer(room);
  stats.gamesFinished++;
  track("game_finished", { code: room.code, reason, winner: room.winner.names.join(","), score: room.winner.score });
  broadcast(room);
}

/* ---------------- per-turn idle timer (auto-pass an AFK player) ---------------- */
function clearTurnTimer(room) { if (room.turnTimer) { clearTimeout(room.turnTimer); room.turnTimer = null; } }
function armTurnTimer(room) {
  clearTurnTimer(room);
  if (!room.started || room.over) return;
  room.turnDeadline = Date.now() + TURN_MS;
  room.turnTimer = setTimeout(() => {
    if (!rooms.has(room.code) || !room.started || room.over || room.busy) return;
    room.up = [];
    room.turn = room.players.length ? (room.turn + 1) % room.players.length : 0;
    armTurnTimer(room);
    track("turn_autopass", { code: room.code });
    broadcast(room);
  }, TURN_MS);
}

/* ---------------- best-effort persistence (survives a restart; single-instance) ---------------- */
let persistDirty = false;
function markDirty() { persistDirty = true; }
function persist() {
  if (!persistDirty) return; persistDirty = false;
  try {
    const data = [];
    rooms.forEach(r => data.push({
      code: r.code, hostId: r.hostId, deck: r.deck, matched: r.matched, turn: r.turn,
      started: r.started, over: r.over, winner: r.winner, deadline: r.deadline, lastActivity: r.lastActivity,
      players: r.players.map(p => ({ id: p.id, name: p.name, score: p.score, avatar: p.avatar || 0 }))
    }));
    fs.writeFileSync(PERSIST_FILE, JSON.stringify(data));
  } catch (e) {}
}
function restore() {
  try {
    if (!fs.existsSync(PERSIST_FILE)) return;
    const data = JSON.parse(fs.readFileSync(PERSIST_FILE, "utf8"));
    const now = Date.now();
    data.forEach(r => {
      const room = {
        code: r.code, hostId: r.hostId, deck: r.deck, matched: r.matched, up: [], turn: r.turn,
        started: r.started, over: r.over, busy: false, winner: r.winner, deadline: r.deadline,
        timer: null, turnTimer: null, turnDeadline: null, lastActivity: now,
        players: r.players.map(p => ({ id: p.id, name: p.name, score: p.score, avatar: p.avatar || 0, conn: null, connected: false, graceTimer: null }))
      };
      rooms.set(room.code, room);
      if (room.started && !room.over) {
        const remaining = (room.deadline || 0) - now;
        if (remaining <= 0) { room.over = true; room.winner = computeWinner(room); }
        else room.timer = setTimeout(() => { if (rooms.has(room.code) && room.started && !room.over) endGame(room, "time"); }, remaining);
      }
    });
    if (rooms.size) track("restored", { rooms: rooms.size });
  } catch (e) {}
}

/* ---------------- message handlers ---------------- */
function onMessage(conn, msg) {
  const t = msg && msg.type;
  if (t === "create") {
    if (rooms.size >= MAX_ROOMS) return conn.send({ type: "error", message: "Server is busy right now. Try again shortly." });
    const pid = token();
    const room = makeRoom(pid);
    conn.pid = pid; conn.room = room;
    room.players.push({ id: pid, name: String(msg.name || "Player").slice(0, 16), score: 0, avatar: clampAvatar(msg.avatar), conn, connected: true, graceTimer: null });
    stats.roomsCreated++; stats.playersJoined++;
    track("room_created", { code: room.code });
    conn.send({ type: "joined", you: pid, code: room.code, isHost: true });
    broadcast(room);
    return;
  }
  if (t === "join") {
    const room = rooms.get(String(msg.code || "").toUpperCase());
    if (!room) return conn.send({ type: "error", message: "Room not found." });
    if (room.started) return conn.send({ type: "error", message: "That game already started." });
    if (room.players.length >= 4) return conn.send({ type: "error", message: "Room is full (max 4)." });
    const pid = token();
    conn.pid = pid; conn.room = room; room.lastActivity = Date.now();
    room.players.push({ id: pid, name: String(msg.name || "Player").slice(0, 16), score: 0, avatar: clampAvatar(msg.avatar), conn, connected: true, graceTimer: null });
    stats.playersJoined++;
    conn.send({ type: "joined", you: pid, code: room.code, isHost: room.hostId === pid });
    broadcast(room);
    return;
  }
  if (t === "rejoin") {
    const room = rooms.get(String(msg.code || "").toUpperCase());
    if (!room) return conn.send({ type: "error", message: "Room no longer exists.", fatal: true });
    const p = room.players.find(x => x.id === msg.pid);
    if (!p) return conn.send({ type: "error", message: "Your seat is gone.", fatal: true });
    if (p.graceTimer) { clearTimeout(p.graceTimer); p.graceTimer = null; }
    p.conn = conn; p.connected = true;
    conn.pid = p.id; conn.room = room; room.lastActivity = Date.now();
    stats.reconnects++;
    track("reconnect", { code: room.code });
    conn.send({ type: "joined", you: p.id, code: room.code, isHost: room.hostId === p.id });
    broadcast(room);
    return;
  }

  const room = conn.room;
  if (!room) return;
  room.lastActivity = Date.now();

  if (t === "start") {
    if (room.hostId !== conn.pid || room.started) return;
    if (room.players.length < 2) return conn.send({ type: "error", message: "Need at least 2 players to start." });
    room.started = true; room.turn = 0; startTimer(room); armTurnTimer(room);
    stats.gamesStarted++;
    track("game_started", { code: room.code, players: room.players.length });
    broadcast(room);
  } else if (t === "flip") {
    handleFlip(conn, msg.index);
  } else if (t === "restart") {
    // any player in the room can trigger a rematch (not just the host)
    room.deck = newDeck(); room.matched = new Array(N).fill(false); room.up = [];
    room.turn = 0; room.over = false; room.busy = false; room.winner = null; room.started = true;
    room.players.forEach(p => p.score = 0);
    startTimer(room); armTurnTimer(room);
    stats.gamesStarted++;
    track("game_started", { code: room.code, players: room.players.length, rematch: true });
    broadcast(room);
  }
}

function handleFlip(conn, index) {
  const room = conn.room;
  if (!room || !room.started || room.over || room.busy) return;
  const cur = room.players[room.turn];
  if (!cur || cur.id !== conn.pid) return;
  if (typeof index !== "number" || index < 0 || index >= N) return;
  if (room.matched[index] || room.up.indexOf(index) >= 0) return;

  room.up.push(index);
  armTurnTimer(room); // player is active — reset their idle clock
  if (room.up.length < 2) { broadcast(room); return; }

  room.busy = true; broadcast(room);
  const a = room.up[0], b = room.up[1];
  const match = room.deck[a] === room.deck[b];
  setTimeout(() => {
    if (!rooms.has(room.code)) return;
    if (match) {
      room.matched[a] = true; room.matched[b] = true; cur.score++;
      if (room.matched.every(Boolean)) { room.up = []; room.busy = false; return endGame(room, "cleared"); }
    } else {
      room.turn = room.players.length ? (room.turn + 1) % room.players.length : 0;
    }
    room.up = []; room.busy = false; armTurnTimer(room); broadcast(room);
  }, RESOLVE_MS);
}

/* ---------------- disconnect + reconnect grace ---------------- */
function onDisconnect(conn) {
  const room = conn.room; conn.room = null;
  if (!room) return;
  const p = room.players.find(x => x.conn === conn);
  if (!p || !p.connected) return;
  p.connected = false;
  broadcast(room); // opponents see "reconnecting…"
  // grace window before the seat is dropped
  p.graceTimer = setTimeout(() => {
    p.graceTimer = null;
    const idx = room.players.findIndex(x => x.id === p.id);
    if (idx < 0) return;
    room.players.splice(idx, 1);
    if (room.players.length === 0) { if (room.timer) clearTimeout(room.timer); rooms.delete(room.code); return; }
    if (room.hostId === p.id) room.hostId = room.players[0].id;
    if (room.turn >= room.players.length) room.turn = 0;
    const connectedLeft = room.players.filter(x => x.connected).length;
    if (room.started && !room.over && connectedLeft < 2) { endGame(room, "opponent_left"); return; }
    broadcast(room);
  }, GRACE_MS);
}

/* ---------------- idle room sweep ---------------- */
setInterval(() => {
  const now = Date.now();
  rooms.forEach((room, code) => {
    // grace timers already reap empty rooms; this only clears long-idle zombies
    if (now - room.lastActivity > ROOM_IDLE_MS) {
      if (room.timer) clearTimeout(room.timer);
      clearTurnTimer(room);
      room.players.forEach(p => { if (p.graceTimer) clearTimeout(p.graceTimer); });
      rooms.delete(code);
      track("room_swept", { code });
    }
  });
  markDirty();
}, 60000);

restore();                       // reload any live rooms from the last run
setInterval(persist, 2000);      // flush changes to disk
process.on("SIGTERM", () => { persistDirty = true; persist(); process.exit(0); });
process.on("SIGINT", () => { persistDirty = true; persist(); process.exit(0); });

server.listen(PORT, () => console.log("ZopMatch multiplayer server on :" + PORT));
