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

const PORT = 4173;
const POLL_MS = 1500;
const CONNECT_TIMEOUT_MS = 25000;
const STREAM_WIDTH = 720;
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

// ---- screen stream + input -------------------------------------------------

async function screenSize(serial) {
  const { out } = await run('adb', ['-s', serial, 'shell', 'wm', 'size']);
  const m = [...out.matchAll(/(\d+)x(\d+)/g)].pop();
  return m ? { w: +m[1], h: +m[2] } : { w: 1080, h: 2400 };
}

// Split screenrecord's Annex-B byte stream into decodable chunks:
// [1 = keyframe (SPS+PPS+IDR) | 0 = delta][bytes]
function makeNalSplitter(emit) {
  let buf = Buffer.alloc(0);
  let config = [];
  let flushTimer = null;

  const handleNal = (nal) => {
    const type = nal[4 + (nal[2] === 1 ? -1 : 0)] & 0x1f; // header byte after start code
    const data = nal;
    if (type === 7 || type === 8) config.push(data);
    else if (type === 5) {
      emit(1, Buffer.concat([...config, data]));
      config = [];
    } else if (type === 1) emit(0, data);
  };

  const findStart = (from) => {
    for (let i = from; i + 3 < buf.length; i++) {
      if (buf[i] === 0 && buf[i + 1] === 0 && (buf[i + 2] === 1 || (buf[i + 2] === 0 && buf[i + 3] === 1))) return i;
    }
    return -1;
  };

  const drain = (final) => {
    let start = findStart(0);
    if (start < 0) return;
    for (;;) {
      const next = findStart(start + 3);
      if (next < 0) {
        if (final) {
          handleNal(buf.subarray(start));
          buf = Buffer.alloc(0);
        } else buf = buf.subarray(start);
        return;
      }
      handleNal(buf.subarray(start, next));
      start = next;
    }
  };

  return (chunk) => {
    buf = Buffer.concat([buf, chunk]);
    drain(false);
    // screenrecord only writes when the screen changes: don't hold the last frame
    clearTimeout(flushTimer);
    flushTimer = setTimeout(() => drain(true), 30);
  };
}

function startStream(ws, serial, size) {
  const outW = STREAM_WIDTH;
  const outH = Math.round((size.h * outW) / size.w / 2) * 2;
  ws.send(JSON.stringify({ t: 'init', w: outW, h: outH }));

  let rec = null;
  let alive = true;
  const launch = () => {
    if (!alive) return;
    rec = spawn('adb', [
      '-s', serial, 'exec-out', 'screenrecord',
      '--output-format=h264', '--size', `${outW}x${outH}`,
      '--bit-rate', '6000000', '--time-limit', '170', '-',
    ]);
    const feed = makeNalSplitter((key, data) => {
      if (ws.readyState === ws.OPEN && ws.bufferedAmount < 2e6) {
        ws.send(Buffer.concat([Buffer.from([key]), data]), { binary: true });
      }
    });
    rec.stdout.on('data', feed);
    rec.on('exit', () => setTimeout(launch, 100)); // screenrecord caps at 3 min: restart
  };
  launch();

  // one long-lived shell so taps/swipes don't pay adb start-up each time
  const sh = spawn('adb', ['-s', serial, 'shell']);
  sh.stdin.on('error', () => {});
  const cmd = (line) => sh.stdin.writable && sh.stdin.write(line + '\n');
  const px = (v, max) => Math.max(0, Math.min(max - 1, Math.round(Number(v) * max)));
  const esc = (s) =>
    String(s).replace(/[\r\n]/g, '').replace(/[^A-Za-z0-9 ]/g, (c) => '\\' + c).replace(/ /g, '%s');

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    const { w, h } = size;
    if (m.t === 'tap') cmd(`input tap ${px(m.x, w)} ${px(m.y, h)}`);
    else if (m.t === 'swipe') {
      const ms = Math.max(30, Math.min(3000, Math.round(Number(m.ms) || 200)));
      cmd(`input swipe ${px(m.x1, w)} ${px(m.y1, h)} ${px(m.x2, w)} ${px(m.y2, h)} ${ms}`);
    } else if (m.t === 'key' && Number.isInteger(m.code)) cmd(`input keyevent ${m.code}`);
    else if (m.t === 'text' && typeof m.s === 'string') cmd(`input text ${esc(m.s)}`);
  });

  ws.on('close', () => {
    alive = false;
    rec && rec.kill();
    sh.kill();
  });
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
  const size = await screenSize(serial);
  wss.handleUpgrade(req, socket, head, (ws) => startStream(ws, serial, size));
});

async function init() {
  if (!(await run('which', ['adb'])).ok) {
    return set('missing', 'brew install android-platform-tools');
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
