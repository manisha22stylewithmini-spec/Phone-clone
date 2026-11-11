// Local agent: pairs with an Android phone over Wi-Fi (QR), streams its screen
// to the browser as H.264 and relays taps / swipes / typing back.
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { pipeline } = require('stream/promises');
const crypto = require('crypto');
const { execFile, spawn } = require('child_process');
const QRCode = require('qrcode');
const { WebSocketServer } = require('ws');
const { startStream } = require('./stream');

const PORT = 4173;
const POLL_MS = 1500;
const CONNECT_TIMEOUT_MS = 25000;
const MAX_UPLOAD = 2 * 1024 ** 3;
const safeName = (n) =>
  path.basename(String(n || 'file')).replace(/[^\w.-]+/g, '_').slice(-100) || 'file';

// Pages allowed to control the phone besides localhost. Add your deployed URL.
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '')
  .split(',')
  .concat(['https://phone-clone-sigma.vercel.app'])
  .filter(Boolean);

const originOk = (origin) => {
  if (!origin) return true; // curl / same-origin navigation
  try {
    const { hostname } = new URL(origin);
    if (hostname === 'localhost' || hostname === '127.0.0.1') return true;
  } catch {
    return false;
  }
  return ALLOWED_ORIGINS.includes(origin);
};

const run = (cmd, args, timeout = 8000) =>
  new Promise((resolve) =>
    execFile(cmd, args, { timeout }, (err, stdout, stderr) =>
      resolve({ ok: !err, out: stdout || '', err: stderr || (err && err.message) || '' })
    )
  );
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// status: missing | waiting | pairing | connecting | connected | error
let state = { status: 'waiting', message: '', qr: '', serial: '' };
let session = null;
let busy = false;

const set = (status, message = '', extra = {}) => {
  state = { ...state, status, message, ...extra };
};

async function connectedSerial() {
  const { out } = await run('adb', ['devices']);
  const line = out.split('\n').slice(1).find((l) => /\sdevice$/.test(l.trim()));
  return line ? line.split(/\s+/)[0] : '';
}

// adb mdns services -> [{ name, type, addr }]
async function mdnsServices() {
  const { out } = await run('adb', ['mdns', 'services']);
  return out
    .split('\n')
    .map((l) => l.trim().split(/\s+/))
    .filter((p) => p.length >= 3 && p[1].startsWith('_adb-tls'))
    .map(([name, type, addr]) => ({ name, type, addr }));
}

async function newSession() {
  const name = 'phoneclone-' + crypto.randomBytes(3).toString('hex');
  const password = String(crypto.randomInt(100000, 999999));
  const qr = await QRCode.toString(`WIFI:T:ADB;S:${name};P:${password};;`, {
    type: 'svg',
    margin: 1,
    width: 280,
  });
  session = { name, password };
  set('waiting', '', { qr, serial: '' });
}

async function pairAndConnect(svc, password = session.password) {
  set('pairing', 'Phone found. Pairing…');
  const pair = await run('adb', ['pair', svc.addr, password], 15000);
  if (!/success/i.test(pair.out + pair.err)) {
    return set('error', 'Pairing failed. Press "New QR code" and scan again.');
  }
  set('connecting', 'Paired. Connecting…');
  const until = Date.now() + CONNECT_TIMEOUT_MS;
  while (Date.now() < until) {
    let serial = await connectedSerial();
    if (!serial) {
      const target = (await mdnsServices()).find((s) => s.type.startsWith('_adb-tls-connect'));
      if (target) await run('adb', ['connect', target.addr], 8000);
      serial = await connectedSerial();
    }
    if (serial) return set('connected', '', { serial });
    await sleep(1000);
  }
  set('error', 'Paired, but could not connect. Is the phone on the same Wi-Fi?');
}

async function tick() {
  if (busy) return;
  busy = true;
  try {
    const serial = await connectedSerial(); // USB cable or already-paired phone
    if (serial) {
      if (state.status !== 'connected') set('connected', '', { serial });
      return;
    }
    if (state.status === 'connected') set('waiting', '', { serial: '' });
    if (state.status !== 'waiting') return;

    const services = await mdnsServices();
    // already-paired phone with Wireless debugging on: reconnect silently
    const known = services.find((s) => s.type.startsWith('_adb-tls-connect'));
    if (known) {
      await run('adb', ['connect', known.addr], 8000);
      if (await connectedSerial()) return;
    }
    const svc = services.find(
      (s) => s.type.startsWith('_adb-tls-pairing') && s.name === session.name
    );
    if (svc) await pairAndConnect(svc);
  } finally {
    busy = false;
  }
}

// ---- http ------------------------------------------------------------------

const json = (res, code, body) => {
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
};

const server = http.createServer(async (req, res) => {
  const origin = req.headers.origin;
  if (!originOk(origin)) {
    res.writeHead(403);
    return res.end();
  }
  if (origin) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Access-Control-Allow-Private-Network', 'true');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Vary', 'Origin');
  }
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    return res.end();
  }

  if (req.url.startsWith('/api/upload') && req.method === 'POST') {
    const serial = state.serial;
    if (!serial || state.status !== 'connected') return json(res, 409, { error: 'Phone is not connected' });
    if (Number(req.headers['content-length'] || 0) > MAX_UPLOAD) return json(res, 413, { error: 'File too large (2 GB max)' });
    const name = safeName(new URL(req.url, 'http://x').searchParams.get('name'));
    const tmp = path.join(os.tmpdir(), `phoneclone-${crypto.randomBytes(4).toString('hex')}-${name}`);
    const remote = `/sdcard/DCIM/PhoneClone/${Date.now().toString(36)}_${name}`;
    try {
      await pipeline(req, fs.createWriteStream(tmp));
      await run('adb', ['-s', serial, 'shell', 'mkdir', '-p', '/sdcard/DCIM/PhoneClone']);
      const push = await run('adb', ['-s', serial, 'push', tmp, remote], 600000);
      if (!push.ok) return json(res, 500, { error: 'Could not copy to the phone' });
      // make it show up in Gallery / photo pickers right away
      await run('adb', ['-s', serial, 'shell', 'am', 'broadcast', '-a',
        'android.intent.action.MEDIA_SCANNER_SCAN_FILE', '-d', `file://${remote}`]);
      return json(res, 200, { ok: true, name, path: remote });
    } catch {
      return json(res, 500, { error: 'Upload failed' });
    } finally {
      fs.unlink(tmp, () => {});
    }
  }
  if (req.url === '/api/state') return json(res, 200, state);
  if (req.url === '/api/retry' && req.method === 'POST') {
    await newSession();
    return json(res, 200, state);
  }
  if (req.url === '/api/pair' && req.method === 'POST') {
    let body = '';
    for await (const c of req) body += c;
    let data = {};
    try { data = JSON.parse(body); } catch {}
    const { addr, code } = data;
    if (!/^[\d.]+:\d+$/.test(addr || '') || !/^\d{6}$/.test(code || '')) {
      return json(res, 400, { error: 'Enter IP:port and the 6-digit code' });
    }
    busy = true;
    pairAndConnect({ addr }, code).finally(() => (busy = false));
    return json(res, 200, state);
  }
  // local copy of the page (same files that are deployed)
  const root = path.join(__dirname, 'public');
  const rel = decodeURIComponent(req.url.split('?')[0]);
  const full = path.normalize(path.join(root, rel === '/' ? 'index.html' : rel));
  if (req.method === 'GET' && full.startsWith(root + path.sep) && fs.existsSync(full) && fs.statSync(full).isFile()) {
    const types = {
      '.html': 'text/html; charset=utf-8', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
      '.webp': 'image/webp', '.svg': 'image/svg+xml', '.gif': 'image/gif',
    };
    res.writeHead(200, { 'Content-Type': types[path.extname(full)] || 'application/octet-stream' });
    return res.end(fs.readFileSync(full));
  }
  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({ noServer: true });
server.on('upgrade', async (req, socket, head) => {
  if (req.url !== '/stream' || !originOk(req.headers.origin) || !state.serial) {
    socket.destroy();
    return;
  }
  const serial = state.serial;
  wss.handleUpgrade(req, socket, head, async (ws) => {
    try {
      await startStream(ws, serial);
    } catch (e) {
      console.error('stream failed:', e.message);
      ws.send(JSON.stringify({ t: 'error', message: e.message }));
      ws.close();
    }
  });
});

async function init() {
  if (!(await run('which', ['adb'])).ok || !(await run('which', ['scrcpy'])).ok) {
    return set('missing', 'brew install android-platform-tools scrcpy');
  }
  await run('adb', ['start-server'], 15000);
  await newSession();
  setInterval(tick, POLL_MS);
}

server.on('error', (e) => {
  if (e.code === 'EADDRINUSE') {
    console.error(`\n  Port ${PORT} is already in use. Phone Clone may already be running: http://localhost:${PORT}\n`);
    process.exit(0);
  }
  throw e;
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`\n  Phone Clone agent running → http://localhost:${PORT}\n`);
  init();
});
