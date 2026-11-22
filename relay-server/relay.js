/* Ретранслятор мультиплеера: WebSocket-сервер без зависимостей.
   Комнаты: в каждой один хост и сколько угодно игроков. Сервер ничего не считает —
   только пересылает сообщения между хостом и игроками. Используется встроенным в exe
   (игра по локальной сети) и отдельно: `node server.js` (игра через интернет).

   Протокол (JSON в текстовых кадрах):
     игрок → сервер: {t:'host', room} | {t:'join', room} | {t:'to', id, d} | {t:'all', d} | {t:'up', d}
     сервер → игрок: {t:'ok', id, role} | {t:'err', m} | {t:'peer', id, on} | {t:'from', id, d} | {t:'d', d} | {t:'bye'} */
'use strict';
const http = require('http');
const crypto = require('crypto');
const os = require('os');

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_FRAME = 4 * 1024 * 1024;

function lanIPs() {
  const out = [];
  for (const list of Object.values(os.networkInterfaces())) for (const a of list || []) if (a.family === 'IPv4' && !a.internal) out.push(a.address);
  return out;
}

function frame(text) {
  const p = Buffer.from(text, 'utf8'), n = p.length;
  let h;
  if (n < 126) { h = Buffer.alloc(2); h[1] = n; }
  else if (n < 65536) { h = Buffer.alloc(4); h[1] = 126; h.writeUInt16BE(n, 2); }
  else { h = Buffer.alloc(10); h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
  h[0] = 0x81;
  return Buffer.concat([h, p]);
}

function start(port, log) {
  log = log || (() => {});
  const rooms = new Map();
  let nextId = 1;
  const server = http.createServer((req, res) => { res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('armorgame relay\n'); });

  server.on('upgrade', (req, sock) => {
    const key = req.headers['sec-websocket-key'];
    if (!key) { sock.destroy(); return; }
    const acc = crypto.createHash('sha1').update(key + GUID).digest('base64');
    sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
    sock.setNoDelay(true);
    const c = { id: nextId++, sock, room: null, role: null, buf: Buffer.alloc(0), parts: [] };
    const send = o => { if (!sock.destroyed) sock.write(frame(JSON.stringify(o))); };
    c.send = send;

    const leave = () => {
      const r = c.room && rooms.get(c.room); if (!r) return;
      if (c.role === 'host') {
        for (const p of r.clients.values()) { p.send({ t: 'bye' }); p.room = null; }
        rooms.delete(c.room); log('комната ' + c.room + ' закрыта');
      } else { r.clients.delete(c.id); if (r.host) r.host.send({ t: 'peer', id: c.id, on: false }); }
      c.room = null;
    };

    const onMsg = text => {
      let m; try { m = JSON.parse(text); } catch (e) { return; }
      if (!m || typeof m !== 'object') return;
      const room = String(m.room || 'lan').slice(0, 40);
      if (m.t === 'host') {
        if (rooms.has(room)) { send({ t: 'err', m: 'Комната «' + room + '» уже занята' }); return; }
        leave(); rooms.set(room, { host: c, clients: new Map() }); c.room = room; c.role = 'host';
        send({ t: 'ok', id: c.id, role: 'host', ips: lanIPs() }); log('хост в комнате ' + room);
      } else if (m.t === 'join') {
        const r = rooms.get(room); if (!r) { send({ t: 'err', m: 'Комната «' + room + '» не найдена — хост ещё не создал бой' }); return; }
        leave(); r.clients.set(c.id, c); c.room = room; c.role = 'client';
        send({ t: 'ok', id: c.id, role: 'client' }); r.host.send({ t: 'peer', id: c.id, on: true });
      } else {
        const r = c.room && rooms.get(c.room); if (!r) return;
        if (c.role === 'host') {
          if (m.t === 'to') { const p = r.clients.get(m.id); if (p) p.send({ t: 'd', d: m.d }); }
          else if (m.t === 'all') { const s = frame(JSON.stringify({ t: 'd', d: m.d })); for (const p of r.clients.values()) if (!p.sock.destroyed) p.sock.write(s); }
        } else if (m.t === 'up') r.host.send({ t: 'from', id: c.id, d: m.d });
      }
    };

    sock.on('data', chunk => {
      c.buf = Buffer.concat([c.buf, chunk]);
      for (;;) {
        const b = c.buf; if (b.length < 2) return;
        const fin = b[0] & 0x80, op = b[0] & 0x0f, masked = b[1] & 0x80;
        let len = b[1] & 0x7f, off = 2;
        if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
        else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
        if (len > MAX_FRAME) { sock.destroy(); return; }
        const mk = masked ? 4 : 0; if (b.length < off + mk + len) return;
        let p = b.subarray(off + mk, off + mk + len);
        if (masked) { const k = b.subarray(off, off + 4); p = Buffer.from(p); for (let i = 0; i < p.length; i++) p[i] ^= k[i & 3]; }
        c.buf = b.subarray(off + mk + len);
        if (op === 8) { sock.end(); return; }
        if (op === 9) { const h = Buffer.from([0x8a, p.length]); sock.write(Buffer.concat([h, p])); continue; }
        if (op === 1 || op === 0) { c.parts.push(p); if (fin) { onMsg(Buffer.concat(c.parts).toString('utf8')); c.parts = []; } }
      }
    });
    sock.on('close', leave);
    sock.on('error', () => { leave(); sock.destroy(); });
  });

  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '0.0.0.0', () => resolve({ server, port, ips: lanIPs() }));
  });
}

module.exports = { start, lanIPs };
