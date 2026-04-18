/**
 * YT Factory — Puppeteer renderer (v2 — fixed-rate screenshot loop)
 *
 * Loads an HTML file (with CSS @keyframes animations) into headless Chromium
 * and captures frames as PNG at a FIXED rate using page.screenshot() in a
 * timed loop.
 *
 * Why fixed-rate screenshot vs CDP screencast (v1):
 *   v1 used Page.startScreencast which only emits frames when Chromium
 *   detects a visual change. This caused two issues in production:
 *   (a) frames cluster during animations (0-3s heavy), then sparse afterwards
 *       — when ffmpeg encodes them at uniform input rate, the sparse periods
 *       look like "stuttering" or "freezing" in the output MP4.
 *   (b) once animations settle, screencast may stop emitting entirely, so
 *       the captured period is shorter than requested.
 *
 *   v2 uses a wall-clock loop with page.screenshot at fixed interval. Each
 *   captured frame is guaranteed to be at a known timestamp. Result: uniform
 *   playback, no stutter, predictable frame count.
 *
 * Usage: node render.js <html_path> <frames_dir> <animation_sec> <target_fps>
 *
 * Notes:
 *   - target_fps is the CAPTURE rate (typical 10-15fps on CPU). The handler
 *     encodes to 30fps output via ffmpeg duplication for smooth playback.
 *   - animation_sec is the ACTIVE capture duration. The handler later pads
 *     the MP4 with a static hold of the last frame to reach the requested
 *     scene duration_sec. So animations get captured fully but viewer also
 *     gets time to read the final composition.
 *   - On CPU 4vCPU/8GB, page.screenshot of 1920x1080 PNG takes ~50-100ms.
 *     Target 15fps (interval 66ms) is the upper bound; 10fps (100ms) is
 *     the safe choice. We attempt the target and report actual rate achieved.
 */

const puppeteer = require('puppeteer-core');
const path = require('path');
const fs = require('fs');

const CHROMIUM_PATH = process.env.CHROMIUM_PATH || '/usr/bin/chromium';
const VIEWPORT = { width: 1920, height: 1080 };
const NAV_TIMEOUT_MS = 30000;

async function render(htmlPath, framesDir, animationSec, targetFps, playbackRate) {
  console.log(`[render] starting (v3 — fixed-rate screenshot + CDP playback rate)`);
  console.log(`  html:          ${htmlPath}`);
  console.log(`  frames:        ${framesDir}`);
  console.log(`  capture:       ${animationSec}s @ ${targetFps}fps target`);
  console.log(`  target frames: ${Math.round(animationSec * targetFps)}`);
  console.log(`  playbackRate:  ${playbackRate}x (1.0=real-time, <1=slower)`);
  console.log(`  chrome:        ${CHROMIUM_PATH}`);

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

    // Apply CSS animation slowdown via DOM mutation BEFORE navigation.
    // Two-layer strategy:
    //   1. CDP Animation.setPlaybackRate — fast path, handled by Chromium
    //      at the animation timeline level (preserves fill-mode semantics)
    //   2. DOM injection — fallback that multiplies animation-duration and
    //      animation-delay for elements where CDP did not take effect.
    //
    // We include BOTH because Chromium versions vary in how well
    // setPlaybackRate applies to CSS @keyframes (vs Web Animations API).
    // IMPORTANT: we do NOT force animation-fill-mode=both blindly anymore.
    // That masked a real HTML template bug (scene_007 Dominion .hero-card
    // had its `.anim-in` fadeSlideIn animation CLOBBERED by an element-
    // specific `animation: heroPulse` declaration, leaving opacity:0 forever).
    // The template bug is fixed at the visual-maestro generator level; see
    // `visual-maestro/references/infographics.md` "Multi-animation CSS rule"
    // and lesson #59 in runpod-infrastructure.md.
    if (playbackRate !== 1.0) {
      const client = await page.createCDPSession();
      try {
        await client.send('Animation.enable');
        await client.send('Animation.setPlaybackRate', { playbackRate });
        console.log(`  CDP Animation.setPlaybackRate(${playbackRate}) set BEFORE navigation`);
      } catch (e) {
        console.warn(`  WARN: CDP setPlaybackRate failed: ${e.message}`);
      }

      // DOM injection as safety net. For double-slowdown safety we check
      // if element has already been touched (data-slowed marker) to avoid
      // compounding the factor if this script somehow runs twice.
      const slowFactor = 1 / playbackRate;
      await page.evaluateOnNewDocument((factor) => {
        const apply = () => {
          document.querySelectorAll('*').forEach(el => {
            if (el.dataset.slowed) return;  // idempotent
            const cs = window.getComputedStyle(el);
            const dur = parseFloat(cs.animationDuration);
            const del = parseFloat(cs.animationDelay);
            if (dur && dur > 0) el.style.animationDuration = `${(dur * factor).toFixed(3)}s`;
            if (del && del > 0) el.style.animationDelay = `${(del * factor).toFixed(3)}s`;
            const tDur = parseFloat(cs.transitionDuration);
            const tDel = parseFloat(cs.transitionDelay);
            if (tDur && tDur > 0) el.style.transitionDuration = `${(tDur * factor).toFixed(3)}s`;
            if (tDel && tDel > 0) el.style.transitionDelay = `${(tDel * factor).toFixed(3)}s`;
            el.dataset.slowed = '1';
          });
        };
        if (document.readyState === 'loading') {
          document.addEventListener('DOMContentLoaded', apply, { once: true });
        } else {
          apply();
        }
      }, slowFactor);
      console.log(`  DOM slowdown injected: factor=${slowFactor}x (idempotent, dataset.slowed marker)`);
    }

    const fileUrl = `file://${path.resolve(htmlPath)}`;
    console.log(`  loading:  ${fileUrl}`);
    await page.goto(fileUrl, {
      waitUntil: 'networkidle0',
      timeout: NAV_TIMEOUT_MS,
    });

    await page.evaluateHandle(() => document.fonts.ready);
    console.log(`  fonts ready`);

    console.log(`  starting capture loop`);
    const intervalMs = 1000 / targetFps;
    const totalFrames = Math.round(animationSec * targetFps);

    // Capture loop — wall-clock targeted intervals
    const startMs = Date.now();
    let actualCaptureMs = 0;
    let totalScreenshotMs = 0;

    for (let i = 1; i <= totalFrames; i++) {
      const targetT = startMs + i * intervalMs;
      const wait = targetT - Date.now();
      if (wait > 0) {
        await new Promise(r => setTimeout(r, wait));
      }

      const tShot = Date.now();
      const buf = await page.screenshot({
        type: 'png',
        omitBackground: false,
        // Skip clip — full viewport is what we want
      });
      totalScreenshotMs += Date.now() - tShot;

      const fname = `frame_${String(i).padStart(5, '0')}.png`;
      await fs.promises.writeFile(path.join(framesDir, fname), buf);
    }

    actualCaptureMs = Date.now() - startMs;
    const realFps = (totalFrames / (actualCaptureMs / 1000)).toFixed(2);
    const avgShotMs = (totalScreenshotMs / totalFrames).toFixed(0);

    console.log(`  captured ${totalFrames} frames in ${actualCaptureMs}ms`);
    console.log(`  real fps:        ${realFps}  (target was ${targetFps})`);
    console.log(`  avg screenshot:  ${avgShotMs}ms per frame`);

    if (realFps < targetFps * 0.7) {
      console.warn(
        `  WARN: real fps ${realFps} < 70% of target ${targetFps} — ` +
        `screenshots are slower than budget. Animations will appear ` +
        `slower than real-time. Consider lowering target_fps.`
      );
    }
  } finally {
    await browser.close().catch(() => {});
  }
}

const [, , htmlPath, framesDir, animationSec, targetFps, playbackRate] = process.argv;

if (!htmlPath || !framesDir || !animationSec || !targetFps) {
  console.error('Usage: node render.js <html_path> <frames_dir> <animation_sec> <target_fps> [playback_rate=1.0]');
  process.exit(2);
}

render(
  htmlPath,
  framesDir,
  parseFloat(animationSec),
  parseInt(targetFps, 10),
  playbackRate ? parseFloat(playbackRate) : 1.0
)
  .then(() => process.exit(0))
  .catch(err => {
    console.error('[render] error:', err && err.stack || err);
    process.exit(1);
  });
