# Phone Clone

Control your Android phone from your laptop, shown inside a phone frame.

The page (`public/`) is static and can be hosted anywhere (e.g. Vercel). A small helper on your
laptop talks to the phone, because a website alone cannot reach it.

## Run

```bash
brew install android-platform-tools scrcpy
npm install
npm start
```

Open http://localhost:4173 (or your deployed page in Chrome/Edge), then on the phone:
**Settings → Developer options → Wireless debugging → Pair device with QR code**, and scan.
A USB cable with USB debugging on also works and is smoother.

To allow your deployed page to control the phone, set `ALLOWED_ORIGINS=https://your-site.vercel.app`.

The screen is streamed with scrcpy's on-device server (low latency) and touch is sent live, so dragging follows your finger.
