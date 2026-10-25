'use strict';
/* Minimal ustar writer/reader with gzip, zero dependencies. Files + directories only. */
const fs = require('fs'), fsp = fs.promises, path = require('path'), zlib = require('zlib');
const { once } = require('events');

const B = 512;
function oct(n, len) { return n.toString(8).padStart(len - 1, '0') + '\0'; }

function header(name, size, mtime, mode, isDir) {
  const h = Buffer.alloc(B);
  let prefix = '';
  if (Buffer.byteLength(name) > 100) {
    const i = name.lastIndexOf('/', name.length - 1 - (isDir ? 1 : 0));
    // find a split so that name<=100 and prefix<=155
    let cut = -1;
    for (let k = name.indexOf('/'); k >= 0; k = name.indexOf('/', k + 1)) {
      if (Buffer.byteLength(name.slice(k + 1)) <= 100 && Buffer.byteLength(name.slice(0, k)) <= 155) { cut = k; break; }
    }
    if (cut < 0) return null;
    prefix = name.slice(0, cut); name = name.slice(cut + 1);
  }
  h.write(name, 0, 100); h.write(oct(mode & 0o7777, 8), 100); h.write(oct(0, 8), 108); h.write(oct(0, 8), 116);
  h.write(oct(size, 12), 124); h.write(oct(Math.floor(mtime / 1000), 12), 136);
  h.write('        ', 148); h.write(isDir ? '5' : '0', 156); h.write('ustar\0', 257); h.write('00', 263);
  if (prefix) h.write(prefix, 345, 155);
  let sum = 0; for (let i = 0; i < B; i++) sum += h[i];
  h.write(sum.toString(8).padStart(6, '0') + '\0 ', 148);
  return h;
}

async function write(ws, buf) { if (!ws.write(buf)) await once(ws, 'drain'); }

/** pack(cwd, targets[], outFile, {filter(relPath)->bool}) */
async function pack(cwd, targets, outFile, opts = {}) {
  const filter = opts.filter || (() => true);
  const gz = zlib.createGzip({ level: 6 });
  const out = fs.createWriteStream(outFile);
  gz.pipe(out);
  const done = new Promise((res, rej) => { out.on('finish', res); out.on('error', rej); gz.on('error', rej); });
  async function add(rel) {
    if (!filter(rel)) return;
    const abs = path.join(cwd, rel);
    let st; try { st = await fsp.lstat(abs); } catch (_) { return; }
    if (st.isDirectory()) {
      const h = header(rel.replace(/\/+$/, '') + '/', 0, st.mtimeMs, st.mode, true);
      if (h) await write(gz, h);
      for (const n of await fsp.readdir(abs)) await add(rel + '/' + n);
    } else if (st.isFile()) {
      const h = header(rel, st.size, st.mtimeMs, st.mode, false);
      if (!h) return;
      await write(gz, h);
      let sent = 0;
      try {
        for await (const chunk of fs.createReadStream(abs)) {
          const room = st.size - sent;
          if (room <= 0) break;
          const c = chunk.length > room ? chunk.subarray(0, room) : chunk;
          sent += c.length; await write(gz, c);
        }
      } catch (_) {}
      if (sent < st.size) await write(gz, Buffer.alloc(st.size - sent)); // file shrank while reading
      const pad = (B - (st.size % B)) % B;
      if (pad) await write(gz, Buffer.alloc(pad));
    }
  }
  for (const t of targets) await add(t.replace(/^\/+|\/+$/g, ''));
  await write(gz, Buffer.alloc(B * 2));
  gz.end();
  await done;
}

/** extract(file, cwd) */
async function extract(file, cwd) {
  cwd = path.resolve(cwd);
  const it = fs.createReadStream(file).pipe(zlib.createGunzip())[Symbol.asyncIterator]();
  let buf = Buffer.alloc(0), eof = false;
  async function fill(n) {
    while (buf.length < n && !eof) {
      const r = await it.next();
      if (r.done) eof = true; else buf = buf.length ? Buffer.concat([buf, r.value]) : r.value;
    }
  }
  while (true) {
    await fill(B);
    if (buf.length < B) break;
    const h = buf.subarray(0, B); buf = buf.subarray(B);
    if (h.every(x => x === 0)) break;
    const str = (a, l) => { const s = h.toString('utf8', a, a + l); const z = s.indexOf('\0'); return z < 0 ? s : s.slice(0, z); };
    let name = str(0, 100); const prefix = str(345, 155); if (prefix) name = prefix + '/' + name;
    const size = parseInt(str(124, 12).trim() || '0', 8) || 0;
    const mode = parseInt(str(100, 8).trim() || '644', 8) || 0o644;
    const type = String.fromCharCode(h[156] || 48);
    const clean = name.replace(/\\/g, '/').replace(/^\/+/, '');
    const bad = !clean || clean.split('/').includes('..');
    const target = path.resolve(cwd, clean);
    const safe = !bad && (target === cwd || target.startsWith(cwd + path.sep));
    const isFile = (type === '0' || type === '\0') && !name.endsWith('/');
    if (type === '5' || name.endsWith('/')) { if (safe) await fsp.mkdir(target, { recursive: true }); continue; }
    let ws = null;
    if (isFile && safe) { await fsp.mkdir(path.dirname(target), { recursive: true }); ws = fs.createWriteStream(target, { mode: (mode & 0o777) | 0o600 }); }
    let left = size;
    while (left > 0) {
      await fill(1);
      if (!buf.length) throw new Error('Truncated archive');
      const take = Math.min(left, buf.length);
      if (ws) await write(ws, buf.subarray(0, take));
      buf = buf.subarray(take); left -= take;
    }
    if (ws) { ws.end(); await once(ws, 'close'); }
    const pad = (B - (size % B)) % B;
    if (pad) { await fill(pad); buf = buf.subarray(Math.min(pad, buf.length)); }
  }
}

module.exports = { pack, extract };
