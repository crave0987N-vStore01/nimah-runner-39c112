'use strict';
/* Persistence layer. Bots (metadata), bot ZIPs and WhatsApp sessions survive runner restarts.
   - MongoDB (MONGODB_URI)  : recommended for free hosts with an ephemeral disk (GridFS for blobs)
   - Local disk (DATA_DIR)  : default; persistent only if the disk/volume is persistent */
const fs = require('fs'), fsp = fs.promises, path = require('path');
const { pipeline } = require('stream/promises');

class FsStore {
  constructor(dir) { this.kind = 'disk'; this.dir = path.join(dir, 'store'); }
  async init() { await fsp.mkdir(path.join(this.dir, 'bots'), { recursive: true }); await fsp.mkdir(path.join(this.dir, 'blobs'), { recursive: true }); }
  _b(id) { return path.join(this.dir, 'bots', id + '.json'); }
  _f(kind, id) { return path.join(this.dir, 'blobs', kind + '-' + id); }
  async listBots() {
    const out = [];
    for (const f of await fsp.readdir(path.join(this.dir, 'bots'))) {
      if (!f.endsWith('.json')) continue;
      try { out.push(JSON.parse(await fsp.readFile(path.join(this.dir, 'bots', f), 'utf8'))); } catch (_) {}
    }
    return out;
  }
  async saveBot(doc) { const t = this._b(doc.id) + '.tmp'; await fsp.writeFile(t, JSON.stringify(doc)); await fsp.rename(t, this._b(doc.id)); }
  async deleteBot(id) { await fsp.rm(this._b(id), { force: true }); }
  async putBlob(kind, id, file) { const t = this._f(kind, id) + '.tmp'; await fsp.copyFile(file, t); await fsp.rename(t, this._f(kind, id)); }
  async getBlobToFile(kind, id, dest) { try { await fsp.copyFile(this._f(kind, id), dest); return true; } catch (_) { return false; } }
  async delBlob(kind, id) { await fsp.rm(this._f(kind, id), { force: true }); }
  async close() {}
}

class MongoStore {
  constructor(uri, dbName) { this.kind = 'mongodb'; this.uri = uri; this.dbName = dbName; }
  async init() {
    const { MongoClient, GridFSBucket } = require('mongodb');
    this.client = new MongoClient(this.uri, { serverSelectionTimeoutMS: 15000, appName: 'nimah-runner' });
    await this.client.connect();
    this.db = this.client.db(this.dbName || undefined);
    this.col = this.db.collection('bots');
    this.buckets = { zip: new GridFSBucket(this.db, { bucketName: 'zips' }), session: new GridFSBucket(this.db, { bucketName: 'sessions' }) };
    await this.db.command({ ping: 1 });
  }
  async listBots() { return (await this.col.find({}).toArray()).map(d => { const { _id, ...r } = d; return { ...r, id: _id }; }); }
  async saveBot(doc) { const { id, ...rest } = doc; await this.col.replaceOne({ _id: id }, rest, { upsert: true }); }
  async deleteBot(id) { await this.col.deleteOne({ _id: id }); }
  async putBlob(kind, id, file) {
    const b = this.buckets[kind];
    const up = b.openUploadStream(id);
    await pipeline(fs.createReadStream(file), up);
    const olds = await b.find({ filename: id }).toArray();
    for (const o of olds) if (String(o._id) !== String(up.id)) { try { await b.delete(o._id); } catch (_) {} }
  }
  async getBlobToFile(kind, id, dest) {
    const b = this.buckets[kind];
    const files = await b.find({ filename: id }).sort({ uploadDate: -1 }).limit(1).toArray();
    if (!files.length) return false;
    await pipeline(b.openDownloadStream(files[0]._id), fs.createWriteStream(dest));
    return true;
  }
  async delBlob(kind, id) {
    const b = this.buckets[kind];
    for (const o of await b.find({ filename: id }).toArray()) { try { await b.delete(o._id); } catch (_) {} }
  }
  async close() { try { await this.client.close(); } catch (_) {} }
}

async function createStore(cfg) {
  if (cfg.mongoUri) {
    try { const s = new MongoStore(cfg.mongoUri, cfg.mongoDb); await s.init(); return { store: s, warn: '' }; }
    catch (e) {
      const fb = new FsStore(cfg.dataDir); await fb.init();
      return { store: fb, warn: 'MongoDB unavailable (' + e.message + ') - using local disk, sessions will NOT survive restarts' };
    }
  }
  const s = new FsStore(cfg.dataDir); await s.init();
  return { store: s, warn: '' };
}

module.exports = { createStore, FsStore, MongoStore };
