'use strict';
/* Minimal streaming ZIP extractor (stored + deflate), zero dependencies. */
const fs = require('fs'), fsp = fs.promises, path = require('path'), zlib = require('zlib');
const { pipeline } = require('stream/promises');

async function readAt(fh, pos, len) {
  const b = Buffer.alloc(len);
  const { bytesRead } = await fh.read(b, 0, len, pos);
  return b.subarray(0, bytesRead);
}

async function listEntries(fh, size) {
  const tail = Math.min(size, 65557);
  const buf = await readAt(fh, size - tail, tail);
  let i = buf.length - 22;
  for (; i >= 0; i--) if (buf.readUInt32LE(i) === 0x06054b50) break;
  if (i < 0) throw new Error('Not a valid ZIP file');
  const total = buf.readUInt16LE(i + 10);
  const cdSize = buf.readUInt32LE(i + 12);
  const cdOff = buf.readUInt32LE(i + 16);
  if (total === 0xffff || cdOff === 0xffffffff || cdSize === 0xffffffff) throw new Error('ZIP64 archives are not supported');
  const cd = await readAt(fh, cdOff, cdSize);
  const out = [];
  let p = 0;
  while (p + 46 <= cd.length && cd.readUInt32LE(p) === 0x02014b50) {
    const flags = cd.readUInt16LE(p + 8), method = cd.readUInt16LE(p + 10);
    const csize = cd.readUInt32LE(p + 20), usize = cd.readUInt32LE(p + 24);
    const nl = cd.readUInt16LE(p + 28), el = cd.readUInt16LE(p + 30), cl = cd.readUInt16LE(p + 32);
    const ext = cd.readUInt32LE(p + 38), off = cd.readUInt32LE(p + 42);
    const name = cd.toString('utf8', p + 46, p + 46 + nl).replace(/\\/g, '/');
    out.push({ name, flags, method, csize, usize, mode: (ext >>> 16) & 0o7777, off, isDir: name.endsWith('/') });
    p += 46 + nl + el + cl;
  }
  return out;
}

/**
 * unzip(zipPath, dest, {filter(name)->bool, maxBytes})
 * Skips entries rejected by filter, refuses path traversal, caps total uncompressed size.
 */
async function unzip(zipPath, dest, opts = {}) {
  const filter = opts.filter || (() => true);
  const maxBytes = opts.maxBytes || 1024 * 1024 * 1024;
  dest = path.resolve(dest);
  await fsp.mkdir(dest, { recursive: true });
  const fh = await fsp.open(zipPath, 'r');
  let count = 0;
  try {
    const st = await fh.stat();
    const entries = await listEntries(fh, st.size);
    if (!entries.length) throw new Error('ZIP file is empty');
    let total = 0;
    for (const e of entries) {
      if (e.flags & 1) throw new Error('Encrypted ZIP files are not supported');
      const name = e.name.replace(/^\/+/, '');
      if (!name || name.split('/').includes('..') || /^[A-Za-z]:/.test(name)) continue;
      if (!filter(name, e.isDir)) continue;
      const target = path.resolve(dest, name);
      if (target !== dest && !target.startsWith(dest + path.sep)) continue;
      if (e.isDir) { await fsp.mkdir(target, { recursive: true }); continue; }
      total += e.usize;
      if (total > maxBytes) throw new Error('ZIP is too large when extracted');
      if (e.method !== 0 && e.method !== 8) throw new Error('Unsupported compression in ' + name);
      await fsp.mkdir(path.dirname(target), { recursive: true });
      const lh = await readAt(fh, e.off, 30);
      if (lh.length < 30 || lh.readUInt32LE(0) !== 0x04034b50) throw new Error('Corrupt ZIP entry: ' + name);
      const start = e.off + 30 + lh.readUInt16LE(26) + lh.readUInt16LE(28);
      if (e.csize === 0 && e.usize === 0) { await fsp.writeFile(target, ''); }
      else {
        const rs = fs.createReadStream(zipPath, { start, end: start + e.csize - 1 });
        const ws = fs.createWriteStream(target);
        if (e.method === 8) await pipeline(rs, zlib.createInflateRaw(), ws);
        else await pipeline(rs, ws);
      }
      const mode = (e.mode & 0o777) || 0o644;
      try { await fsp.chmod(target, mode | 0o600); } catch (_) {}
      count++;
    }
  } finally { await fh.close(); }
  return count;
}

module.exports = { unzip };
