'use strict';
/* Preloaded into every bot process (NODE_OPTIONS=--require). Reports WhatsApp QR / pairing / connection
   state to the runner over loopback. Supports Baileys (+forks), whatsapp-web.js and qrcode-terminal.
   Must never throw or change bot behaviour. */
(function () {
  try {
    var base = process.env.NIMAH_HOOK_URL, tok = process.env.NIMAH_HOOK_TOKEN;
    if (!base || !tok) return;
    if (/[\\/](npm-cli\.js|npx-cli\.js|corepack)|[\\/]bin[\\/]npm$/.test(process.argv[1] || '')) return;
    var http = require('http'), U = new URL(base), lastQr = '';

    function send(ev) {
      try {
        var body = JSON.stringify(ev);
        if (ev.t === 'qr') { if (body === lastQr) return; lastQr = body; }
        var r = http.request({
          hostname: U.hostname, port: U.port, path: '/internal/event', method: 'POST', agent: false, timeout: 3000,
          headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body), 'x-hook-token': tok }
        });
        r.on('error', function () {}); r.on('timeout', function () { r.destroy(); });
        r.end(body);
      } catch (e) {}
    }

    function attach(sock) {
      if (!sock || !sock.ev || typeof sock.ev.on !== 'function' || sock.__nh) return;
      sock.__nh = 1;
      var num = String(process.env.NIMAH_PAIR_NUMBER || '').replace(/\D/g, ''), tried = false;
      sock.ev.on('connection.update', function (u) {
        try {
          if (u.qr) {
            send({ t: 'qr', qr: u.qr });
            if (num && !tried && typeof sock.requestPairingCode === 'function' &&
                sock.authState && sock.authState.creds && !sock.authState.creds.registered) {
              tried = true;
              setTimeout(function () {
                Promise.resolve(sock.requestPairingCode(num))
                  .then(function (c) { send({ t: 'pair', code: String(c) }); })
                  .catch(function (e) { send({ t: 'log', msg: 'pairing code request failed: ' + (e && e.message) }); });
              }, 1500);
            }
          }
          if (u.connection) {
            var ev = { t: 'state', s: u.connection };
            if (u.connection === 'open' && sock.user) ev.user = String(sock.user.id || '').split(':')[0].split('@')[0];
            if (u.connection === 'close') { var er = u.lastDisconnect && u.lastDisconnect.error; ev.code = er && er.output && er.output.statusCode; }
            send(ev);
          }
        } catch (e) {}
      });
    }

    function wrapFn(fn) {
      if (typeof fn !== 'function' || fn.__nhw) return fn;
      var w = function () { var s = fn.apply(this, arguments); try { attach(s); } catch (e) {} return s; };
      try { Object.setPrototypeOf(w, fn); } catch (e) {}
      try { Object.keys(fn).forEach(function (k) { try { w[k] = fn[k]; } catch (e) {} }); } catch (e) {}
      w.__nhw = true;
      return w;
    }

    var NAMES = { 'default': 1, makeWASocket: 1, makeWaSocket: 1 };
    function patchBaileys(m) {
      if (!m) return m;
      if (typeof m === 'function') {
        var w = wrapFn(m);
        Object.keys(NAMES).forEach(function (k) { if (typeof m[k] === 'function') w[k] = m[k] === m ? w : wrapFn(m[k]); });
        return w;
      }
      if (typeof m !== 'object') return m;
      var cache = {};
      return new Proxy({}, {
        get: function (_, k) { var v = m[k]; if (NAMES[k] && typeof v === 'function') return cache[k] || (cache[k] = wrapFn(v)); return v; },
        has: function (_, k) { return k in m; },
        ownKeys: function () { return Reflect.ownKeys(m); },
        getOwnPropertyDescriptor: function (_, k) { var d = Object.getOwnPropertyDescriptor(m, k); if (d) d.configurable = true; return d; },
        set: function (_, k, v) { m[k] = v; return true; }
      });
    }

    function patchQrt(m) {
      if (!m || m.__nh || typeof m.generate !== 'function') return;
      m.__nh = 1; var g = m.generate;
      m.generate = function (input) { try { if (typeof input === 'string' && input.length > 10) send({ t: 'qr', qr: input }); } catch (e) {} return g.apply(this, arguments); };
    }

    function patchWweb(m) {
      var C = m && m.Client;
      if (!C || !C.prototype || C.prototype.__nh) return;
      var oe = C.prototype.emit;
      C.prototype.emit = function (name, a) {
        try {
          if (name === 'qr') send({ t: 'qr', qr: String(a) });
          else if (name === 'ready') send({ t: 'state', s: 'open', user: this.info && this.info.wid && this.info.wid.user });
          else if (name === 'authenticated') send({ t: 'state', s: 'connecting' });
          else if (name === 'disconnected') send({ t: 'state', s: 'close' });
        } catch (e) {}
        return oe.apply(this, arguments);
      };
      C.prototype.__nh = 1;
    }

    var Module = require('module'), orig = Module._load, seen = new WeakMap();
    var BPKG = /^(@[^\/]+\/)?[^\/]*baileys[^\/]*$/i;
    var BFILE = /node_modules[\\\/](@[^\\\/]+[\\\/])?[^\\\/]*baileys[^\\\/]*[\\\/]lib[\\\/]index\.js$/i;
    Module._load = function (request) {
      var m = orig.apply(this, arguments);
      try {
        if (typeof request !== 'string' || !m) return m;
        var isObj = typeof m === 'object' || typeof m === 'function';
        if (isObj && seen.has(m)) return seen.get(m);
        var out = m;
        if (BPKG.test(request) || BFILE.test(request)) out = patchBaileys(m);
        else if (request === 'qrcode-terminal' || /node_modules[\\\/]qrcode-terminal[\\\/]lib[\\\/]main\.js$/.test(request)) patchQrt(m);
        else if (request === 'whatsapp-web.js' || /node_modules[\\\/]whatsapp-web\.js[\\\/]index\.js$/.test(request)) patchWweb(m);
        if (isObj && out !== m) seen.set(m, out);
        return out;
      } catch (e) { return m; }
    };
  } catch (e) {}
})();
