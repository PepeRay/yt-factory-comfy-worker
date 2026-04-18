/**
 * YT Factory — Puppeteer renderer
 *
 * Loads an HTML file (with CSS @keyframes animations) into headless Chromium
 * and captures frames as PNG images for ffmpeg to encode into MP4.
 *
 * Capture strategy: CDP `Page.startScreencast`. Chromium emits a frame
 * whenever it detects a visual change. To keep emitting frames after the
 * CSS animations settle (so the MP4 has constant duration matching the
 * scene narration), we inject an off-screen DOM ticker that triggers a tiny
 * repaint every 16ms (~60fps), forcing Chromium to keep producing frames.
 *
 * We hard-cap captured frames at `targetFrames = duration * fps` so the
 * stream never overshoots even if Chromium emits faster than real-time.
 *
 * Usage: node render.js <html_path> <frames_dir> <duration_sec> <fps>
 *
 * Requires:
 *   - puppeteer-core (npm) — control protocol only, no bundled Chromium
 *   - System Chromium at $CHROMIUM_PATH (default /usr/bin/chromium)
 */

const puppeteer = require('puppeteer-core');
const path = require('path');
const fs = require('fs');

const CHROMIUM_PATH = process.env.CHROMIUM_PATH || '/usr/bin/chromium';
const VIEWPORT = { width: 1920, height: 1080 };
const NAV_TIMEOUT_MS = 30000;

async function render(htmlPath, framesDir, durationSec, fps) {
  console.log(`[render] starting`);
  console.log(`  html:    ${htmlPath}`);
  console.log(`  frames:  ${framesDir}`);
  console.log(`  target:  ${Math.round(durationSec * fps)} frames @ ${fps}fps (${durationSec}s)`);
  console.log(`  chrome:  ${CHROMIUM_PATH}`);

  const browser = await puppeteer.launch({
    executablePath: CHROMIUM_PATH,
    headless: 'new',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--no-first-run',
      '--no-zygote',
      '--font-render-hinting=none',
      '--disable-background-timer-throttling',
      '--disable-backgrounding-occluded-windows',
      '--disable-renderer-backgrounding',
      `--window-size=${VIEWPORT.width},${VIEWPORT.height}`,
    ],
  });

  try {
    const page = await browser.newPage();
    await page.setViewport({
      width: VIEWPORT.width,
      height: VIEWPORT.height,
      deviceScaleFactor: 1,
    });

    // Load HTML from local file
    const fileUrl = `file://${path.resolve(htmlPath)}`;
    console.log(`  loading: ${fileUrl}`);
    await page.goto(fileUrl, {
      waitUntil: 'networkidle0',
      timeout: NAV_TIMEOUT_MS,
    });

    // Wait for fonts to fully load (Google Fonts CDN typically used)
    await page.evaluateHandle(() => document.fonts.ready);
    console.log(`  fonts ready`);

    // Inject off-screen ticker to force constant repaints — without this,
    // Chromium stops emitting frames once CSS animations settle (e.g. after
    // the last fadeSlideIn finishes), and the MP4 would be too short.
    await page.evaluate(() => {
      const tick = document.createElement('div');
      tick.style.cssText =
        'position:fixed;top:-100px;left:-100px;width:1px;height:1px;' +
        'opacity:0.001;pointer-events:none;z-index:-1;';
      document.body.appendChild(tick);
      let n = 0;
      setInterval(() => {
        n = (n + 1) % 1000;
        tick.style.transform = `translateX(${n * 0.01}px)`;
      }, 16);
    });

    // Set up CDP screencast
    const client = await page.createCDPSession();

    let frameIdx = 0;
    const writePromises = [];
    const targetFrames = Math.round(durationSec * fps);

    client.on('Page.screencastFrame', async ({ data, sessionId }) => {
      // Cap at target — extra frames after this point are dropped (we already
      // have enough for the requested duration)
      if (frameIdx >= targetFrames) {
        try {
          await client.send('Page.screencastFrameAck', { sessionId });
        } catch (e) {
          // session may be closing
        }
        return;
      }
      frameIdx++;
      const buf = Buffer.from(data, 'base64');
      const fname = `frame_${String(frameIdx).padStart(5, '0')}.png`;
      const fpath = path.join(framesDir, fname);
      writePromises.push(fs.promises.writeFile(fpath, buf));
      try {
        await client.send('Page.screencastFrameAck', { sessionId });
      } catch (e) {
        // session may have closed
      }
    });

    await client.send('Page.startScreencast', {
      format: 'png',
      quality: 100,
      maxWidth: VIEWPORT.width,
      maxHeight: VIEWPORT.height,
      everyNthFrame: 1, // request maximum framerate
    });

    // Capture window: duration + small buffer for warmup/flush
    const captureMs = Math.round(durationSec * 1000) + 300;
    console.log(`  capturing for ${captureMs}ms...`);
    await new Promise(r => setTimeout(r, captureMs));

    await client.send('Page.stopScreencast');

    // Let any in-flight frames finish writing
    await new Promise(r => setTimeout(r, 200));
    await Promise.all(writePromises);

    console.log(`  captured: ${frameIdx} frames (target was ${targetFrames})`);
    if (frameIdx < targetFrames * 0.8) {
      console.warn(
        `  WARN: only ${frameIdx} / ${targetFrames} frames captured ` +
        `(${Math.round((frameIdx / targetFrames) * 100)}%)`
      );
    }
  } finally {
    await browser.close().catch(() => {});
  }
}

const [, , htmlPath, framesDir, durationSec, fps] = process.argv;

if (!htmlPath || !framesDir || !durationSec || !fps) {
  console.error('Usage: node render.js <html_path> <frames_dir> <duration_sec> <fps>');
  process.exit(2);
}

render(htmlPath, framesDir, parseFloat(durationSec), parseInt(fps, 10))
  .then(() => process.exit(0))
  .catch(err => {
    console.error('[render] error:', err && err.stack || err);
    process.exit(1);
  });
