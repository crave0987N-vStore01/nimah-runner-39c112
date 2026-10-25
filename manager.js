'use strict';
const fs = require('fs'), fsp = fs.promises, path = require('path'), crypto = require('crypto');
const { spawn } = require('child_process');
const { unzip } = require('./lib/zip');
const tar = require('./lib/tar');

const ANSI = /\x1b\[[0-9;?]*[A-Za-z]/g;
const SESSION_RE = /^(sessions?|auth|auth[-_]?info.*|baileys[-_]?(auth|store|session).*|\.wwebjs_auth|wa[-_]?sessions?|.*[-_]sessions?|sessions?[-_].*|creds\.json|sessions?\.json|.*[-_]creds\.json|app-state.*)$/i;
const SKIP_RE = /(^|\/)(Cache|Code Cache|GPUCache|GrShaderCache|ShaderCache|DawnCache|Service Worker|blob_storage|Crashpad|Crash Reports|component_crx_cache|BrowserMetrics[^/]*|Singleton[^/]*|lockfile|[^/]*\.log)(\/|$)/;
const JUNK = /(^|\/)(node_modules|\.git|__MACOSX)(\/|$)|(^|\/)(\.DS_Store|Thumbs\.db)$/;
const MAX_SESSION = 64 * 1024 * 1024;
const BLOCKED_ENV = /^(PATH|LD_.*|NIMAH_.*|RUNNER_SECRET|MONGODB_URI|DATA_DIR|HOME)$/i;

const rid = () => crypto.randomBytes(6).toString('hex');
const sha1 = s => crypto.createHash('sha1').update(s).digest('hex');
const exists = p => { try { fs.accessSync(p); return true; } catch (_) { return false; } };
const readJson = p => { try { return JSON.parse(fs.readFileSync(p, 'utf8')); } catch (_) { return null; } };

function pipeLines(stream, cb) {
  let buf = '';
  stream.setEncoding('utf8');
  stream.on('data', d => {
    buf += d; let i;
    while ((i = buf.indexOf('\n')) >= 0) { cb(buf.slice(0, i).replace(/\r/g, '')); buf = buf.slice(i + 1); }
    if (buf.length > 8192) { cb(buf); buf = ''; }
  });
  stream.on('end', () => { if (buf) cb(buf.replace(/\r/g, '')); });
}

function cleanEnv(obj) {
  const out = {};
  if (!obj || typeof obj !== 'object') return out;
  for (const [k, v] of Object.entries(obj)) {
    if (!/^[A-Za-z_][A-Za-z0-9_]{0,63}$/.test(k) || BLOCKED_ENV.test(k)) continue;
    out[k] = String(v == null ? '' : v).slice(0, 8192);
    if (Object.keys(out).length >= 100) break;
  }
  return out;
}

class Manager {
  constructor(o) {
    this.o = o; this.store = o.store; this.dir = o.dataDir;
    this.bots = new Map(); this.tokens = new Map();
    this.installQ = Promise.resolve(); this.nextPort = 4100;
    this.key = crypto.createHash('sha256').update('nimah-env|' + (o.envKey || o.secret)).digest();
    this.hookPath = path.join(__dirname, 'hook.js');
  }

  /* ---------- env encryption ---------- */
  enc(obj) {
    const iv = crypto.randomBytes(12), c = crypto.createCipheriv('aes-256-gcm', this.key, iv);
    const ct = Buffer.concat([c.update(JSON.stringify(obj), 'utf8'), c.final()]);
    return ['v1', iv.toString('base64url'), c.getAuthTag().toString('base64url'), ct.toString('base64url')].join('.');
  }
  dec(s) {
    if (!s) return {};
    try {
      const [v, iv, tag, ct] = s.split('.');
      if (v !== 'v1') return {};
      const d = crypto.createDecipheriv('aes-256-gcm', this.key, Buffer.from(iv, 'base64url'));
      d.setAuthTag(Buffer.from(tag, 'base64url'));
      return JSON.parse(Buffer.concat([d.update(Buffer.from(ct, 'base64url')), d.final()]).toString('utf8'));
    } catch (_) { return null; }
  }

  /* ---------- lifecycle ---------- */
  async init() {
    await fsp.mkdir(path.join(this.dir, 'bots'), { recursive: true });
    await fsp.mkdir(path.join(this.dir, 'tmp'), { recursive: true });
    for (const f of await fsp.readdir(path.join(this.dir, 'tmp'))) fsp.rm(path.join(this.dir, 'tmp', f), { force: true, recursive: true }).catch(() => {});
    for (const doc of await this.store.listBots()) this.mount(doc);
    this.timer = setInterval(() => this.tick(), 20000);
    this.autoStartAll().catch(() => {});
  }

  mount(doc) {
    let env = this.dec(doc.envEnc); const envBad = env === null; if (envBad) env = {};
    const rt = {
      doc, env, status: doc.hasZip ? 'stopped' : 'empty', wa: '', qr: null, pair: null, user: null,
      proc: null, logs: [], seq: 0, subs: new Set(), crashes: 0, startedAt: 0, stopping: false, waOpened: false,
      chain: Promise.resolve(), timer: null, port: this.nextPort++, hookToken: null, sig: '', lastUp: 0, uploading: false, lastErr: ''
    };
    this.bots.set(doc.id, rt);
    if (envBad) this.log(rt, '⚠ saved environment variables could not be decrypted (RUNNER_SECRET changed?). Re-enter them in Settings.', 'e');
    return rt;
  }

  lock(rt, fn) { const p = rt.chain.then(() => fn()); rt.chain = p.catch(() => {}); return p; }
  installLock(fn) { const p = this.installQ.then(fn); this.installQ = p.catch(() => {}); return p; }
  get(id) { return this.bots.get(id) || null; }
  appDir(rt) { return path.join(this.dir, 'bots', rt.doc.id, 'app'); }
  runDir(rt) { return path.join(this.appDir(rt), rt.doc.root || ''); }
  async save(rt) { rt.doc.updatedAt = Date.now(); await this.store.saveBot(rt.doc); }

  async autoStartAll() {
    for (const rt of this.bots.values()) {
      if (rt.doc.desired !== 'running' || !rt.doc.hasZip) continue;
      this.log(rt, '↺ runner restarted - restoring bot automatically', 's');
      await this.lock(rt, () => this.startInner(rt)).catch(e => this.fail(rt, e));
      await new Promise(r => setTimeout(r, 2000));
    }
  }

  /* ---------- views / events ---------- */
  view(rt) {
    const live = rt.status === 'running' && rt.startedAt;
    return {
      id: rt.doc.id, name: rt.doc.name, status: rt.status, wa: rt.wa, user: rt.user, hasQr: !!rt.qr,
      pairCode: rt.pair, pairNumber: rt.doc.pairNumber || '', desired: rt.doc.desired, restarts: rt.crashes,
      uptime: live ? Math.floor((Date.now() - rt.startedAt) / 1000) : 0, envKeys: Object.keys(rt.env),
      startCmd: rt.doc.startCmd || '', persist: rt.doc.persist || [], createdAt: rt.doc.createdAt, updatedAt: rt.doc.updatedAt,
      hasZip: !!rt.doc.hasZip, lastError: rt.lastErr || ''
    };
  }
  list() { return [...this.bots.values()].map(r => this.view(r)).sort((a, b) => b.createdAt - a.createdAt); }
  emit(rt, evt, data) {
    const msg = 'event: ' + evt + '\ndata: ' + JSON.stringify(data) + '\n\n';
    for (const res of rt.subs) { try { res.write(msg); } catch (_) {} }
  }
  setStatus(rt, s) { rt.status = s; this.emit(rt, 'state', this.view(rt)); }
  fail(rt, e) { rt.lastErr = (e && e.message) || String(e); this.log(rt, '✖ ' + rt.lastErr, 'e'); this.setStatus(rt, 'error'); }

  log(rt, line, src) {
    line = String(line).replace(ANSI, '');
    if (!line.trim()) return;
    const ent = { n: ++rt.seq, t: Date.now(), s: src || 'o', l: line.slice(0, 2000) };
    rt.logs.push(ent); if (rt.logs.length > 1500) rt.logs.splice(0, rt.logs.length - 1500);
    this.emit(rt, 'log', ent);
    if (!rt.pair) {
      const m = line.match(/(?:pairing|pair|linking|login)\s*code\D{0,25}\b([A-Z0-9]{4}[- ]?[A-Z0-9]{4})\b/i);
      if (m && /\d/.test(m[1]) && src !== 's') { rt.pair = m[1].toUpperCase().replace(' ', '-'); this.emit(rt, 'state', this.view(rt)); }
    }
  }
  logsSince(rt, since) { return rt.logs.filter(e => e.n > since); }
  subscribe(rt, res) {
    rt.subs.add(res);
    res.write('retry: 3000\nevent: snapshot\ndata: ' + JSON.stringify({ bot: this.view(rt), logs: rt.logs.slice(-300), qr: rt.qr }) + '\n\n');
    res.on('close', () => rt.subs.delete(res));
  }

  handleEvent(token, ev) {
    const rt = this.tokens.get(token); if (!rt || !ev) return false;
    if (ev.t === 'qr' && typeof ev.qr === 'string' && ev.qr.length < 4096) {
      rt.qr = ev.qr; rt.wa = 'qr'; this.emit(rt, 'qr', { qr: rt.qr }); this.emit(rt, 'state', this.view(rt));
    } else if (ev.t === 'pair' && ev.code) {
      rt.pair = String(ev.code).slice(0, 20); this.log(rt, '🔑 Pairing code: ' + rt.pair, 's'); this.emit(rt, 'state', this.view(rt));
    } else if (ev.t === 'state') {
      if (ev.s === 'open') {
        rt.wa = 'open'; rt.qr = null; rt.pair = null; rt.waOpened = true; rt.crashes = 0;
        if (ev.user) rt.user = String(ev.user).slice(0, 32);
        this.log(rt, '✅ WhatsApp connected' + (rt.user ? ' as +' + rt.user : ''), 's');
        setTimeout(() => this.flush(rt, true), 5000);
      } else if (ev.s === 'connecting') { if (rt.wa !== 'qr') rt.wa = 'connecting'; }
      else if (ev.s === 'close') { rt.wa = 'closed'; rt.qr = null; this.log(rt, '⚠ WhatsApp connection closed' + (ev.code ? ' (code ' + ev.code + ')' : ''), 's'); }
      this.emit(rt, 'state', this.view(rt));
    } else if (ev.t === 'log' && ev.msg) this.log(rt, String(ev.msg), 's');
    return true;
  }

  /* ---------- CRUD ---------- */
  async create({ name, env, startCmd, persist }) {
    if (this.bots.size >= this.o.maxBots) throw Object.assign(new Error('Bot limit reached (' + this.o.maxBots + ')'), { code: 400 });
    name = String(name || '').trim().slice(0, 40);
    if (name.length < 2) throw Object.assign(new Error('Bot name is too short'), { code: 400 });
    const doc = {
      id: rid(), name, envEnc: this.enc(cleanEnv(env)), startCmd: String(startCmd || '').slice(0, 300),
      persist: this.cleanPersist(persist), desired: 'stopped', root: '', hasZip: false, pairNumber: '', createdAt: Date.now(), updatedAt: Date.now()
    };
    await this.store.saveBot(doc);
    const rt = this.mount(doc);
    this.log(rt, 'Bot created. Upload your ZIP to deploy it.', 's');
    return this.view(rt);
  }

  cleanPersist(p) {
    if (typeof p === 'string') p = p.split(/[,\n]/);
    if (!Array.isArray(p)) return [];
    return [...new Set(p.map(x => String(x).trim().replace(/\\/g, '/').replace(/^\.?\/+|\/+$/g, ''))
      .filter(x => x && !x.split('/').includes('..') && !/^node_modules/.test(x) && x.length < 120))].slice(0, 20);
  }

  async update(rt, p) {
    if (typeof p.name === 'string' && p.name.trim().length >= 2) rt.doc.name = p.name.trim().slice(0, 40);
    if (p.envSet || p.envUnset) {
      const e = Object.assign({}, rt.env, cleanEnv(p.envSet));
      for (const k of (Array.isArray(p.envUnset) ? p.envUnset : [])) delete e[k];
      rt.env = e; rt.doc.envEnc = this.enc(e);
    }
    if (typeof p.startCmd === 'string') rt.doc.startCmd = p.startCmd.slice(0, 300);
    if (p.persist !== undefined) rt.doc.persist = this.cleanPersist(p.persist);
    await this.save(rt);
    this.emit(rt, 'state', this.view(rt));
    return this.view(rt);
  }

  async remove(rt) {
    this.killInstall(rt);
    return this.lock(rt, async () => {
      clearTimeout(rt.timer); rt.doc.desired = 'stopped';
      await this.stopProc(rt);
      await this.store.deleteBot(rt.doc.id);
      await this.store.delBlob('zip', rt.doc.id).catch(() => {});
      await this.store.delBlob('session', rt.doc.id).catch(() => {});
      await fsp.rm(path.join(this.dir, 'bots', rt.doc.id), { recursive: true, force: true });
      this.emit(rt, 'gone', {}); for (const r of rt.subs) { try { r.end(); } catch (_) {} }
      this.bots.delete(rt.doc.id);
    });
  }

  /* ---------- start / stop ---------- */
  start(rt) {
    clearTimeout(rt.timer); rt.crashes = 0;
    return this.lock(rt, () => this.startInner(rt)).catch(e => this.fail(rt, e));
  }
  killInstall(rt) { if (rt.installProc) { try { process.kill(-rt.installProc.pid, 'SIGKILL'); } catch (_) {} } }
  stop(rt) {
    this.killInstall(rt);
    return this.lock(rt, async () => {
      clearTimeout(rt.timer); rt.doc.desired = 'stopped'; await this.save(rt);
      await this.stopProc(rt);
      await this.flush(rt, true);
      this.setStatus(rt, rt.doc.hasZip ? 'stopped' : 'empty');
      this.log(rt, '■ stopped', 's');
    }).catch(e => this.fail(rt, e));
  }
  restart(rt) {
    return this.lock(rt, async () => {
      clearTimeout(rt.timer); await this.stopProc(rt); await this.flush(rt, true); rt.crashes = 0;
      await this.startInner(rt);
    }).catch(e => this.fail(rt, e));
  }

  async startInner(rt) {
    if (rt.proc) return;
    if (!rt.doc.hasZip) throw new Error('Upload a ZIP first');
    const active = [...this.bots.values()].filter(b => b !== rt && ['starting', 'installing', 'running'].includes(b.status)).length;
    if (active >= this.o.maxRunning) throw new Error('Runner limit reached (' + this.o.maxRunning + ' running bots). Stop another bot first.');
    rt.stopping = false; rt.lastErr = '';
    rt.doc.desired = 'running'; await this.save(rt);
    this.setStatus(rt, 'starting');
    await this.ensureApp(rt);
    await this.ensureInstall(rt);
    this.spawnBot(rt);
  }

  async stopProc(rt) {
    const p = rt.proc; if (!p) return;
    rt.stopping = true;
    const done = new Promise(r => p.once('exit', r));
    try { process.kill(-p.pid, 'SIGTERM'); } catch (_) { try { p.kill('SIGTERM'); } catch (_) {} }
    const t = setTimeout(() => { try { process.kill(-p.pid, 'SIGKILL'); } catch (_) {} }, 8000);
    await done; clearTimeout(t);
  }

  async shutdown() {
    clearInterval(this.timer);
    await Promise.race([
      Promise.all([...this.bots.values()].map(async rt => { clearTimeout(rt.timer); if (rt.proc) { await this.stopProc(rt); await this.flush(rt, true); } })),
      new Promise(r => setTimeout(r, 20000))
    ]);
  }

  /* ---------- files ---------- */
  async ensureApp(rt) {
    const app = this.appDir(rt);
    if (exists(app) && fs.readdirSync(app).length) return;
    this.log(rt, '⬇ restoring bot files from storage…', 's');
    const tmp = path.join(this.dir, 'tmp', 'z-' + rt.doc.id + '-' + rid() + '.zip');
    const ok = await this.store.getBlobToFile('zip', rt.doc.id, tmp);
    if (!ok) throw new Error('Stored ZIP is missing - please upload your bot again');
    try { await this.extract(rt, tmp); } finally { fsp.rm(tmp, { force: true }).catch(() => {}); }
    await this.restoreSession(rt);
  }

  async extract(rt, zipPath) {
    const app = this.appDir(rt);
    await fsp.rm(app, { recursive: true, force: true });
    await fsp.mkdir(app, { recursive: true });
    const n = await unzip(zipPath, app, { filter: n => !JUNK.test(n), maxBytes: this.o.maxUnzipMb * 1048576 });
    // flatten single wrapper folders
    for (let i = 0; i < 3; i++) {
      if (exists(path.join(app, 'package.json'))) break;
      const ents = fs.readdirSync(app);
      if (ents.length === 1 && fs.statSync(path.join(app, ents[0])).isDirectory()) {
        const inner = path.join(app, ents[0]);
        for (const e of fs.readdirSync(inner)) fs.renameSync(path.join(inner, e), path.join(app, e));
        fs.rmdirSync(inner);
      } else break;
    }
    // locate package.json (shallowest, depth<=3)
    let root = null;
    const walk = (d, depth) => {
      if (root !== null) return;
      if (exists(path.join(d, 'package.json'))) { root = path.relative(app, d); return; }
      if (depth >= 3) return;
      for (const e of fs.readdirSync(d)) { const p = path.join(d, e); if (fs.statSync(p).isDirectory()) walk(p, depth + 1); }
    };
    walk(app, 0);
    if (root === null) throw new Error('No package.json found in the ZIP - this must be a Node.js bot project');
    rt.doc.root = root.split(path.sep).join('/'); await this.save(rt);
    this.log(rt, '📦 extracted ' + n + ' files' + (root ? ' (project in ' + root + '/)' : ''), 's');
  }

  async ensureInstall(rt) {
    const run = this.runDir(rt), pj = path.join(run, 'package.json');
    const pkg = readJson(pj);
    if (!pkg) throw new Error('package.json is missing or invalid');
    const hash = sha1(JSON.stringify([pkg.dependencies, pkg.optionalDependencies, pkg.name, pkg.version]));
    const marker = path.join(run, 'node_modules', '.nimah-hash');
    try { if (fs.readFileSync(marker, 'utf8') === hash) return; } catch (_) {}
    await this.installLock(async () => {
      this.setStatus(rt, 'installing');
      this.log(rt, '⚙ installing dependencies (npm install)… this can take a few minutes', 's');
      const base = ['install', '--omit=dev', '--no-audit', '--no-fund', '--no-progress', '--loglevel=warn'];
      let c = await this.runLogged(rt, 'npm', base, run, 15 * 60000);
      if (c !== 0 && !rt.stopping) { this.log(rt, '↻ retrying npm install with --legacy-peer-deps', 's'); c = await this.runLogged(rt, 'npm', base.concat('--legacy-peer-deps'), run, 15 * 60000); }
      if (c !== 0) throw new Error('npm install failed (exit ' + c + ') - check the logs');
      await fsp.mkdir(path.join(run, 'node_modules'), { recursive: true });
      await fsp.writeFile(marker, hash);
      this.log(rt, '✓ dependencies installed', 's');
    });
  }

  baseEnv(rt) {
    const home = path.join(this.dir, 'bots', rt.doc.id, 'home'); fs.mkdirSync(home, { recursive: true });
    const e = {
      PATH: process.env.PATH, HOME: home, LANG: 'C.UTF-8', TZ: process.env.TZ || 'UTC',
      npm_config_cache: path.join(this.dir, '.npm'), npm_config_update_notifier: 'false', npm_config_fund: 'false',
      PUPPETEER_SKIP_DOWNLOAD: 'true', PUPPETEER_SKIP_CHROMIUM_DOWNLOAD: 'true', HUSKY: '0', ADBLOCK: '1', DISABLE_OPENCOLLECTIVE: '1'
    };
    for (const k of ['PUPPETEER_EXECUTABLE_PATH', 'CHROME_PATH', 'FFMPEG_PATH']) if (process.env[k]) e[k] = process.env[k];
    return e;
  }

  runLogged(rt, cmd, args, cwd, timeout) {
    return new Promise(res => {
      const p = spawn(cmd, args, { cwd, env: this.baseEnv(rt), detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
      rt.installProc = p;
      pipeLines(p.stdout, l => this.log(rt, l, 'i')); pipeLines(p.stderr, l => this.log(rt, l, 'i'));
      const t = setTimeout(() => { this.log(rt, '⏱ timeout', 'e'); try { process.kill(-p.pid, 'SIGKILL'); } catch (_) {} }, timeout);
      p.on('error', e => { clearTimeout(t); this.log(rt, 'spawn error: ' + e.message, 'e'); res(1); });
      p.on('exit', c => { clearTimeout(t); rt.installProc = null; res(c == null ? 1 : c); });
    });
  }

  resolveCmd(rt, run) {
    if (rt.doc.startCmd) return ['sh', ['-c', rt.doc.startCmd]];
    const pkg = readJson(path.join(run, 'package.json')) || {};
    const s = pkg.scripts && pkg.scripts.start;
    if (s && !/\b(pm2|forever)\b/.test(s)) return ['sh', ['-c', 'exec npm start --silent']];
    if (pkg.main && exists(path.join(run, pkg.main))) return ['node', [pkg.main]];
    for (const f of ['index.js', 'main.js', 'bot.js', 'app.js', 'server.js', 'start.js', 'src/index.js'])
      if (exists(path.join(run, f))) return ['node', [f]];
    throw new Error('Could not find how to start the bot. Add a "start" script to package.json or set a start command.');
  }

  spawnBot(rt) {
    const run = this.runDir(rt), cmd = this.resolveCmd(rt, run);
    const token = crypto.randomBytes(16).toString('hex');
    rt.hookToken = token; this.tokens.set(token, rt);
    const env = Object.assign(this.baseEnv(rt), {
      NODE_ENV: 'production', PORT: String(rt.port),
      NIMAH_HOOK_URL: 'http://127.0.0.1:' + this.o.port, NIMAH_HOOK_TOKEN: token, NIMAH_PAIR_NUMBER: rt.doc.pairNumber || ''
    }, rt.env);
    env.NODE_OPTIONS = ['--require=' + this.hookPath, '--max-old-space-size=' + this.o.botMemMb, rt.env.NODE_OPTIONS || ''].filter(Boolean).join(' ');
    const p = spawn(cmd[0], cmd[1], { cwd: run, env, detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
    rt.proc = p; rt.startedAt = Date.now(); rt.wa = ''; rt.qr = null; rt.pair = null; rt.waOpened = false; rt.stopping = false;
    this.log(rt, '▶ starting: ' + cmd[0] + ' ' + cmd[1].join(' '), 's');
    pipeLines(p.stdout, l => this.log(rt, l, 'o')); pipeLines(p.stderr, l => this.log(rt, l, 'o'));
    p.on('error', e => { this.log(rt, 'spawn error: ' + e.message, 'e'); });
    p.on('exit', (code, sig) => this.onExit(rt, p, code, sig));
    rt.sig = rt.sig || this.signatureOf(rt);
    this.setStatus(rt, 'running');
  }

  onExit(rt, p, code, sig) {
    if (rt.proc !== p) return;
    rt.proc = null; this.tokens.delete(rt.hookToken);
    const ran = Date.now() - rt.startedAt; rt.startedAt = 0; rt.qr = null;
    if (rt.wa === 'open') rt.wa = 'closed';
    this.log(rt, '■ process exited (' + (sig || 'code ' + code) + ') after ' + Math.round(ran / 1000) + 's', 's');
    if (rt.stopping) { this.setStatus(rt, 'stopped'); return; }
    if (ran > 60000 || rt.waOpened) rt.crashes = 0;
    rt.crashes++;
    if (rt.doc.desired !== 'running') { this.setStatus(rt, 'stopped'); return; }
    if (rt.crashes > 8) { rt.lastErr = 'Crashed too many times - fix the bot and start it again'; this.log(rt, '✖ ' + rt.lastErr, 'e'); this.setStatus(rt, 'error'); return; }
    const delay = Math.min(60000, 2000 * 2 ** (rt.crashes - 1));
    this.log(rt, '↻ auto-restart in ' + Math.round(delay / 1000) + 's (attempt ' + rt.crashes + ')', 's');
    this.setStatus(rt, 'crashed');
    rt.timer = setTimeout(() => { rt.timer = null; this.lock(rt, () => this.startInner(rt)).catch(e => this.fail(rt, e)); }, delay);
  }

  /* ---------- ZIP ingest ---------- */
  ingest(rt, tmpZip, autostart) {
    return this.lock(rt, async () => {
      try {
        const wasRunning = !!rt.proc || rt.doc.desired === 'running';
        clearTimeout(rt.timer);
        this.setStatus(rt, 'installing');
        this.log(rt, '📥 ZIP received (' + (fs.statSync(tmpZip).size / 1048576).toFixed(1) + ' MB)', 's');
        if (rt.proc) { await this.stopProc(rt); await this.flush(rt, true); }
        await this.store.putBlob('zip', rt.doc.id, tmpZip);
        rt.doc.hasZip = true; await this.save(rt);
        await fsp.rm(this.appDir(rt), { recursive: true, force: true });
        rt.sig = '';
        await this.ensureApp(rt);
        if (autostart || wasRunning) { rt.crashes = 0; await this.startInner(rt); }
        else { this.setStatus(rt, 'stopped'); }
      } catch (e) { this.fail(rt, e); }
      finally { fsp.rm(tmpZip, { force: true }).catch(() => {}); }
    });
  }

  /* ---------- session persistence ---------- */
  sessionTargets(rt, run) {
    const out = new Set();
    const custom = rt.doc.persist || [];
    if (!custom.length) {
      try { for (const n of fs.readdirSync(run)) if (n !== 'node_modules' && SESSION_RE.test(n)) out.add(n); } catch (_) {}
    }
    for (const p of custom) if (exists(path.join(run, p))) out.add(p);
    return [...out];
  }
  signatureOf(rt) {
    const run = this.runDir(rt), targets = this.sessionTargets(rt, run);
    let h = crypto.createHash('md5'), n = 0;
    const walk = rel => {
      if (n > 20000 || SKIP_RE.test(rel)) return;
      let st; try { st = fs.lstatSync(path.join(run, rel)); } catch (_) { return; }
      n++;
      if (st.isDirectory()) { try { for (const c of fs.readdirSync(path.join(run, rel))) walk(rel + '/' + c); } catch (_) {} }
      else if (st.isFile()) h.update(rel + '|' + st.size + '|' + Math.floor(st.mtimeMs) + ';');
    };
    targets.forEach(walk);
    return targets.length ? h.digest('hex') : '';
  }
  async flush(rt, force) {
    if (rt.uploading || !rt.doc.hasZip) return;
    const run = this.runDir(rt); if (!exists(run)) return;
    const targets = this.sessionTargets(rt, run); if (!targets.length) return;
    const sig = this.signatureOf(rt); if (sig === rt.sig) return;
    rt.uploading = true;
    const tmp = path.join(this.dir, 'tmp', 's-' + rt.doc.id + '-' + rid() + '.tgz');
    try {
      await tar.pack(run, targets, tmp, { filter: p => !SKIP_RE.test(p) });
      const size = fs.statSync(tmp).size;
      if (size > MAX_SESSION) this.log(rt, '⚠ session is larger than 64 MB - not saved', 'e');
      else { await this.store.putBlob('session', rt.doc.id, tmp); rt.sig = sig; rt.lastUp = Date.now(); this.log(rt, '💾 session saved (' + Math.max(1, Math.round(size / 1024)) + ' KB)', 's'); }
    } catch (e) { this.log(rt, '⚠ session save failed: ' + e.message, 'e'); }
    finally { rt.uploading = false; fsp.rm(tmp, { force: true }).catch(() => {}); }
  }
  async restoreSession(rt) {
    const tmp = path.join(this.dir, 'tmp', 'r-' + rt.doc.id + '-' + rid() + '.tgz');
    try {
      const ok = await this.store.getBlobToFile('session', rt.doc.id, tmp);
      if (!ok) return false;
      await tar.extract(tmp, this.runDir(rt));
      rt.sig = this.signatureOf(rt);
      this.log(rt, '🔐 saved WhatsApp session restored', 's');
      return true;
    } catch (e) { this.log(rt, '⚠ session restore failed: ' + e.message, 'e'); return false; }
    finally { fsp.rm(tmp, { force: true }).catch(() => {}); }
  }
  tick() { for (const rt of this.bots.values()) if (rt.proc && Date.now() - rt.lastUp > 45000) this.flush(rt, false).catch(() => {}); }

  resetSession(rt) {
    return this.lock(rt, async () => {
      const was = rt.doc.desired === 'running';
      clearTimeout(rt.timer); await this.stopProc(rt);
      await this.store.delBlob('session', rt.doc.id).catch(() => {});
      const run = this.runDir(rt);
      for (const t of this.sessionTargets(rt, run)) await fsp.rm(path.join(run, t), { recursive: true, force: true }).catch(() => {});
      rt.sig = ''; rt.user = null; rt.wa = ''; rt.qr = null; rt.pair = null;
      this.log(rt, '🗑 WhatsApp session cleared - a new QR code will be generated', 's');
      if (was) { rt.crashes = 0; await this.startInner(rt); } else this.setStatus(rt, rt.doc.hasZip ? 'stopped' : 'empty');
    }).catch(e => this.fail(rt, e));
  }

  setPair(rt, number) {
    rt.doc.pairNumber = String(number || '').replace(/\D/g, '').slice(0, 20);
    return this.save(rt).then(() => {
      this.log(rt, rt.doc.pairNumber ? '🔗 pairing-code mode for +' + rt.doc.pairNumber + ' (restarting)' : 'pairing-code mode disabled', 's');
      return this.restart(rt);
    });
  }
}

module.exports = { Manager };
