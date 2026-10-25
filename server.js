'use strict';
/* Nimah Bot Runner - HTTP API.
   Admin calls are authenticated with RUNNER_SECRET (sent by the Vercel backend only).
   Browser calls (ZIP upload, live log stream) use short-lived HMAC tickets minted by the backend. */
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const { createStore } = require('./store');
const { Manager } = require('./manager');

const SECRET = process.env.RUNNER_SECRET || '';
const PORT = parseInt(process.env.PORT || '7860', 10);
const DATA_DIR = path.resolve(process.env.DATA_DIR || path.join(__dirname, 'data'));
const ORIGIN = process.env.ALLOWED_ORIGIN || '*';
const MAX_ZIP = (parseInt(process.env.MAX_ZIP_MB || '100', 10) || 100) * 1048576;
const VERSION = '1.0.0';

if (SECRET.length < 16) { console.error('FATAL: set RUNNER_SECRET (min 16 chars)'); process.exit(1); }

const safeEq = (a, b) => { const x = Buffer.from(String(a)), y = Buffer.from(String(b)); return x.length === y.length && crypto.timingSafeEqual(x, y); };
const cors = () => ({
  'Access-Control-Allow-Origin': ORIGIN, 'Access-Control-Allow-Methods': 'GET,POST,PUT,PATCH,DELETE,OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization', 'Access-Control-Max-Age': '600', 'Vary': 'Origin'
});
function send(res, code, obj) {
  const b = JSON.stringify(obj);
  res.writeHead(code, Object.assign({ 'Content-Type': 'application/json', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' }, cors()));
  res.end(b);
}
const err = (res, code, msg) => send(res, code, { ok: false, error: msg });
function body(req, max = 1 << 20) {
  return new Promise((resolve, reject) => {
    const ch = []; let n = 0;
    req.on('data', c => { n += c.length; if (n > max) { reject(Object.assign(new Error('Body too large'), { code: 413 })); req.destroy(); } else ch.push(c); });
    req.on('end', () => { try { resolve(n ? JSON.parse(Buffer.concat(ch).toString('utf8')) : {}); } catch (_) { reject(Object.assign(new Error('Invalid JSON'), { code: 400 })); } });
    req.on('error', reject);
  });
}

/* failed-auth throttle (per IP) */
const fails = new Map();
const ipOf = req => String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
function throttled(ip) { const f = fails.get(ip); return f && f.n >= 12 && Date.now() - f.t < 60000; }
function failed(ip) { const f = fails.get(ip); if (!f || Date.now() - f.t > 60000) fails.set(ip, { n: 1, t: Date.now() }); else f.n++; }

function isAdmin(req) {
  const h = req.headers['authorization'] || '';
  return h.startsWith('Bearer ') && safeEq(h.slice(7), SECRET);
}
const usedTickets = new Map();
function verifyTicket(t, scope, id, consume) {
  try {
    const [p, sig] = String(t || '').split('.');
    if (!p || !sig) return false;
    const want = crypto.createHmac('sha256', SECRET).update('tk|' + p).digest('base64url');
    if (!safeEq(sig, want)) return false;
    const o = JSON.parse(Buffer.from(p, 'base64url').toString('utf8'));
    if (o.s !== scope || o.b !== id || !(o.e > Date.now())) return false;
    if (consume) {
      for (const [k, e] of usedTickets) if (e < Date.now()) usedTickets.delete(k);
      if (usedTickets.has(o.j)) return false;
      usedTickets.set(o.j, o.e);
    }
    return true;
  } catch (_) { return false; }
}

let mgr = null, ready = false, storeInfo = { kind: '', warn: '' };

const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://x');
  const p = u.pathname.replace(/\/+$/, '') || '/';
  const m = req.method;
  try {
    if (m === 'OPTIONS') { res.writeHead(204, cors()); return res.end(); }
    if (p === '/health') return send(res, 200, { ok: true, ready, t: Date.now() });
    if (p === '/') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('Nimah Bot Runner ' + VERSION); }

    /* loopback hook events from bot processes */
    if (p === '/internal/event' && m === 'POST') {
      const ip = req.socket.remoteAddress || '';
      if (!/^(::1|127\.0\.0\.1|::ffff:127\.0\.0\.1)$/.test(ip) || req.headers['x-forwarded-for']) return err(res, 403, 'forbidden');
      const ev = await body(req, 16384);
      const ok = mgr && mgr.handleEvent(String(req.headers['x-hook-token'] || ''), ev);
      return send(res, ok ? 200 : 403, { ok: !!ok });
    }

    const ip = ipOf(req);
    if (throttled(ip)) return err(res, 429, 'Too many attempts');
    const mm = p.match(/^\/bots\/([a-f0-9]{12})\/(upload|stream)$/);
    const ticketRoute = mm && ((mm[2] === 'upload' && m === 'PUT') || (mm[2] === 'stream' && m === 'GET'));

    if (ticketRoute) {
      const id = mm[1], t = u.searchParams.get('t');
      if (!isAdmin(req) && !verifyTicket(t, mm[2], id, mm[2] === 'upload')) { failed(ip); return err(res, 401, 'Invalid or expired ticket'); }
      if (!ready) return err(res, 503, 'Runner is starting, retry in a few seconds');
      const rt = mgr.get(id); if (!rt) return err(res, 404, 'Bot not found');

      if (mm[2] === 'stream') {
        if (rt.subs.size >= 10) return err(res, 429, 'Too many viewers');
        res.writeHead(200, Object.assign({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' }, cors()));
        mgr.subscribe(rt, res);
        const hb = setInterval(() => { try { res.write(': hb\n\n'); } catch (_) {} }, 15000);
        res.on('close', () => clearInterval(hb));
        return;
      }
      /* upload */
      const len = parseInt(req.headers['content-length'] || '0', 10);
      if (len > MAX_ZIP) return err(res, 413, 'ZIP is larger than ' + (MAX_ZIP / 1048576) + ' MB');
      const tmp = path.join(DATA_DIR, 'tmp', 'up-' + id + '-' + crypto.randomBytes(4).toString('hex') + '.zip');
      let n = 0, aborted = false;
      const ws = fs.createWriteStream(tmp);
      req.on('data', c => { n += c.length; if (n > MAX_ZIP && !aborted) { aborted = true; req.unpipe(ws); ws.destroy(); fs.rm(tmp, { force: true }, () => {}); err(res, 413, 'ZIP is too large'); } });
      req.pipe(ws);
      ws.on('finish', () => {
        if (aborted) return;
        try {
          const fd = fs.openSync(tmp, 'r'), h = Buffer.alloc(4); fs.readSync(fd, h, 0, 4, 0); fs.closeSync(fd);
          if (!(h[0] === 0x50 && h[1] === 0x4b) || n < 22) { fs.rmSync(tmp, { force: true }); return err(res, 400, 'That is not a valid ZIP file'); }
        } catch (_) { return err(res, 500, 'Upload failed'); }
        mgr.ingest(rt, tmp, u.searchParams.get('autostart') !== '0');
        send(res, 202, { ok: true, bot: mgr.view(rt) });
      });
      ws.on('error', () => { if (!res.headersSent) err(res, 500, 'Upload failed'); });
      req.on('aborted', () => { ws.destroy(); fs.rm(tmp, { force: true }, () => {}); });
      return;
    }

    /* ---- admin API ---- */
    if (!isAdmin(req)) { failed(ip); return err(res, 401, 'Unauthorized'); }
    if (!ready) return err(res, 503, 'Runner is starting, retry in a few seconds');

    if (p === '/info' && m === 'GET') {
      const bots = [...mgr.bots.values()];
      return send(res, 200, { ok: true, version: VERSION, store: storeInfo.kind, storeWarning: storeInfo.warn, node: process.version,
        uptime: Math.floor(process.uptime()), bots: bots.length, running: bots.filter(b => b.proc).length, maxBots: mgr.o.maxBots, maxRunning: mgr.o.maxRunning,
        mem: Math.round(process.memoryUsage().rss / 1048576) });
    }
    if (p === '/bots' && m === 'GET') return send(res, 200, { ok: true, bots: mgr.list() });
    if (p === '/bots' && m === 'POST') return send(res, 201, { ok: true, bot: await mgr.create(await body(req)) });

    const bm = p.match(/^\/bots\/([a-f0-9]{12})(?:\/([a-z]+))?$/);
    if (bm) {
      const rt = mgr.get(bm[1]); if (!rt) return err(res, 404, 'Bot not found');
      const act = bm[2] || '';
      if (!act && m === 'GET') return send(res, 200, { ok: true, bot: mgr.view(rt) });
      if (!act && m === 'PATCH') return send(res, 200, { ok: true, bot: await mgr.update(rt, await body(req)) });
      if (!act && m === 'DELETE') { await mgr.remove(rt); return send(res, 200, { ok: true }); }
      if (act === 'logs' && m === 'GET') return send(res, 200, { ok: true, logs: mgr.logsSince(rt, parseInt(u.searchParams.get('since') || '0', 10) || 0), bot: mgr.view(rt), qr: rt.qr });
      if (m === 'POST') {
        if (act === 'start') { mgr.start(rt); return send(res, 202, { ok: true, bot: mgr.view(rt) }); }
        if (act === 'stop') { mgr.stop(rt); return send(res, 202, { ok: true, bot: mgr.view(rt) }); }
        if (act === 'restart') { mgr.restart(rt); return send(res, 202, { ok: true, bot: mgr.view(rt) }); }
        if (act === 'reset') { mgr.resetSession(rt); return send(res, 202, { ok: true, bot: mgr.view(rt) }); }
        if (act === 'pair') { const b = await body(req); mgr.setPair(rt, b.number); return send(res, 202, { ok: true, bot: mgr.view(rt) }); }
      }
    }
    return err(res, 404, 'Not found');
  } catch (e) {
    if (!res.headersSent) err(res, e.code && e.code >= 400 && e.code < 600 ? e.code : 500, e.message || 'Server error');
    else try { res.end(); } catch (_) {}
  }
});
server.requestTimeout = 0; server.headersTimeout = 30000; server.keepAliveTimeout = 65000;

(async () => {
  server.listen(PORT, '0.0.0.0', () => console.log('[runner] listening on :' + PORT));
  const { store, warn } = await createStore({ mongoUri: process.env.MONGODB_URI, mongoDb: process.env.MONGODB_DB, dataDir: DATA_DIR });
  storeInfo = { kind: store.kind, warn };
  if (warn) console.warn('[runner] ' + warn);
  mgr = new Manager({
    store, dataDir: DATA_DIR, secret: SECRET, envKey: process.env.ENV_KEY, port: PORT,
    maxBots: parseInt(process.env.MAX_BOTS || '5', 10), maxRunning: parseInt(process.env.MAX_RUNNING || '3', 10),
    maxUnzipMb: parseInt(process.env.MAX_UNZIP_MB || '600', 10), botMemMb: parseInt(process.env.BOT_MEM_MB || '320', 10)
  });
  await mgr.init();
  ready = true;
  console.log('[runner] ready - store=' + store.kind + ' bots=' + mgr.bots.size);

  const self = process.env.KEEPALIVE_URL || process.env.RENDER_EXTERNAL_URL ||
    (process.env.SPACE_HOST && 'https://' + process.env.SPACE_HOST) ||
    (process.env.KOYEB_PUBLIC_DOMAIN && 'https://' + process.env.KOYEB_PUBLIC_DOMAIN) ||
    (process.env.RAILWAY_PUBLIC_DOMAIN && 'https://' + process.env.RAILWAY_PUBLIC_DOMAIN) || '';
  if (self) { console.log('[runner] keep-alive ping ->', self); setInterval(() => fetch(self.replace(/\/$/, '') + '/health').catch(() => {}), 4 * 60 * 1000).unref(); }
})().catch(e => { console.error('[runner] fatal', e); process.exit(1); });

let down = false;
async function bye() { if (down) return; down = true; console.log('[runner] shutting down'); try { server.close(); if (mgr) await mgr.shutdown(); } catch (_) {} process.exit(0); }
process.on('SIGTERM', bye); process.on('SIGINT', bye);
process.on('uncaughtException', e => console.error('[runner] uncaught', e));
process.on('unhandledRejection', e => console.error('[runner] unhandled', e));
