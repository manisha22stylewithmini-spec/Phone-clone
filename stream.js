// Low-latency screen + live input for one phone, using scrcpy's on-device server.
// Video: H.264 packets -> websocket. Input: touch/key/text over scrcpy's control socket.
const net = require('net');
const fs = require('fs');
const crypto = require('crypto');
const { execFile, spawn } = require('child_process');

const REMOTE_JAR = '/data/local/tmp/scrcpy-server.jar';
const MAX_SIZE = 1024;
const BIT_RATE = 4_000_000;
const MAX_FPS = 60;

const run = (cmd, args, timeout = 15000) =>
  new Promise((resolve) =>
    execFile(cmd, args, { timeout }, (err, stdout, stderr) =>
      resolve({ ok: !err, out: stdout || '', err: stderr || (err && err.message) || '' })
    )
  );
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let tools = null; // { jar, version }
async function findTools() {
  if (tools) return tools;
  const prefix = (await run('brew', ['--prefix', 'scrcpy'])).out.trim();
  const jar = [`${prefix}/share/scrcpy/scrcpy-server`, '/opt/homebrew/share/scrcpy/scrcpy-server', '/usr/local/share/scrcpy/scrcpy-server']
    .find((p) => p && fs.existsSync(p));
  const version = /scrcpy\s+(\S+)/.exec((await run('scrcpy', ['--version'])).out)?.[1];
  if (!jar || !version) throw new Error('scrcpy is not installed (brew install scrcpy)');
  return (tools = { jar, version });
}

// Reads the video socket: 1 dummy byte, 64-byte device name, 4-byte codec id, then 12-byte-header packets.
// Header with bit 63 set = "session" (size change): u32 flag, u32 width, u32 height.
// Otherwise: u64 pts+flags (bit 62 = config, bit 61 = keyframe), u32 payload size.
function videoParser({ onReady, onMeta, onPacket }) {
  let buf = Buffer.alloc(0);
  let stage = 'dummy';
  return (chunk) => {
    buf = Buffer.concat([buf, chunk]);
    for (;;) {
      if (stage === 'dummy') {
        if (buf.length < 1) return;
        buf = buf.subarray(1);
        stage = 'meta';
        onReady();
      } else if (stage === 'meta') {
        if (buf.length < 68) return;
        buf = buf.subarray(68);
        stage = 'packets';
      } else {
        if (buf.length < 12) return;
        if (buf[0] & 0x80) {
          onMeta({ w: buf.readUInt32BE(4), h: buf.readUInt32BE(8) });
          buf = buf.subarray(12);
          continue;
        }
        const size = buf.readUInt32BE(8);
        if (buf.length < 12 + size) return;
        const flags = buf.readBigUInt64BE(0);
        onPacket({ config: !!((flags >> 62n) & 1n), key: !!((flags >> 61n) & 1n) }, buf.subarray(12, 12 + size));
        buf = buf.subarray(12 + size);
      }
    }
  };
}

// Connects through the adb tunnel. The tunnel accepts instantly, so "ready" means the dummy byte arrived.
async function openVideo(port, handlers, giveUpMs = 12000) {
  const until = Date.now() + giveUpMs;
  while (Date.now() < until) {
    const sock = await new Promise((resolve) => {
      const s = net.connect(port, '127.0.0.1');
      let ready = false;
      const feed = videoParser({ ...handlers, onReady: () => { ready = true; resolve(s); } });
      s.on('data', feed);
      s.on('error', () => {});
      s.on('close', () => { if (!ready) resolve(null); else handlers.onClose(); });
    });
    if (sock) return sock;
    await sleep(200);
  }
  throw new Error('Phone did not start the screen service');
}

// Control messages (scrcpy protocol)
const msg = {
  touch(action, x, y, w, h) {
    const b = Buffer.alloc(32);
    b.writeUInt8(2, 0);                    // INJECT_TOUCH_EVENT
    b.writeUInt8(action, 1);               // 0 down, 1 up, 2 move
    b.writeBigInt64BE(-1n, 2);             // pointer id (mouse)
    b.writeInt32BE(Math.round(x * w), 10);
    b.writeInt32BE(Math.round(y * h), 14);
    b.writeUInt16BE(w, 18);
    b.writeUInt16BE(h, 20);
    b.writeUInt16BE(action === 1 ? 0 : 0xffff, 22); // pressure
    b.writeInt32BE(0, 24);                 // action button
    b.writeInt32BE(action === 1 ? 0 : 1, 28); // buttons
    return b;
  },
  key(action, code) {
    const b = Buffer.alloc(14);
    b.writeUInt8(0, 0);                    // INJECT_KEYCODE
    b.writeUInt8(action, 1);               // 0 down, 1 up
    b.writeInt32BE(code, 2);
    b.writeInt32BE(0, 6);                  // repeat
    b.writeInt32BE(0, 10);                 // meta state
    return b;
  },
  text(s) {
    const t = Buffer.from(s, 'utf8').subarray(0, 300);
    const b = Buffer.alloc(5 + t.length);
    b.writeUInt8(1, 0);                    // INJECT_TEXT
    b.writeUInt32BE(t.length, 1);
    t.copy(b, 5);
    return b;
  },
};

async function startStream(ws, serial) {
  const { jar, version } = await findTools();
  const scid = crypto.randomInt(0x7fffffff).toString(16).padStart(8, '0');

  const push = await run('adb', ['-s', serial, 'push', jar, REMOTE_JAR]);
  if (!push.ok) throw new Error('Could not copy the screen service to the phone');
  const fwd = await run('adb', ['-s', serial, 'forward', 'tcp:0', `localabstract:scrcpy_${scid}`]);
  const port = parseInt(fwd.out, 10);
  if (!port) throw new Error('Could not open the connection to the phone');

  const server = spawn('adb', [
    '-s', serial, 'shell',
    `CLASSPATH=${REMOTE_JAR} app_process / com.genymobile.scrcpy.Server ${version} scid=${scid} log_level=info` +
    ` tunnel_forward=true audio=false control=true max_size=${MAX_SIZE} video_bit_rate=${BIT_RATE} max_fps=${MAX_FPS}` +
    ' stay_awake=true clipboard_autosync=false',
  ]);
  let serverLog = '';
  server.stdout.on('data', (d) => (serverLog += d));
  server.stderr.on('data', (d) => (serverLog += d));

  let alive = true;
  let size = { w: 0, h: 0 };
  let config = null;
  let video = null;
  let control = null;

  const stop = () => {
    if (!alive) return;
    alive = false;
    video && video.destroy();
    control && control.destroy();
    server.kill();
    run('adb', ['-s', serial, 'forward', '--remove', `tcp:${port}`]);
    if (ws.readyState === ws.OPEN) ws.close();
  };
  ws.on('close', stop);
  server.on('exit', stop);

  try {
    video = await openVideo(port, {
      onMeta: (m) => {
        size = m;
        ws.send(JSON.stringify({ t: 'init', w: m.w, h: m.h }));
      },
      onPacket: (flags, data) => {
        if (flags.config) { config = Buffer.from(data); return; }
        if (ws.readyState !== ws.OPEN) return;
        // keyframes carry the SPS/PPS so the browser decoder can start on any of them
        const payload = flags.key && config ? Buffer.concat([config, data]) : data;
        ws.send(Buffer.concat([Buffer.from([flags.key ? 1 : 0]), payload]), { binary: true });
      },
      onClose: stop,
    });
    control = net.connect(port, '127.0.0.1');
    control.on('error', () => {});
    control.on('data', () => {}); // device messages (clipboard etc.) are not used
  } catch (e) {
    const detail = serverLog.trim().split('\n').slice(-3).join(' | ');
    stop();
    throw new Error(e.message + (detail ? ` (${detail})` : ''));
  }

  const send = (b) => control && control.writable && control.write(b);
  const clamp01 = (v) => Math.max(0, Math.min(1, Number(v) || 0));

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (!size.w) return;
    if (m.t === 'touch' && [0, 1, 2].includes(m.a)) send(msg.touch(m.a, clamp01(m.x), clamp01(m.y), size.w, size.h));
    else if (m.t === 'key' && Number.isInteger(m.code)) { send(msg.key(0, m.code)); send(msg.key(1, m.code)); }
    else if (m.t === 'text' && typeof m.s === 'string') send(msg.text(m.s));
  });
}

module.exports = { startStream };
