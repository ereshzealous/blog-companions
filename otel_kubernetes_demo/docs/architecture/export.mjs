import { chromium } from 'playwright';
import { readFileSync, writeFileSync, readdirSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const DIR = dirname(fileURLToPath(import.meta.url));
const files = readdirSync(DIR).filter(f => f.endsWith('.excalidraw'));

const browser = await chromium.launch({ channel: 'chrome', headless: true });
const page = await browser.newPage();
// any http(s) origin works for dynamic ESM import; esm.sh itself is fine
await page.goto('https://esm.sh/', { waitUntil: 'domcontentloaded' });

for (const f of files) {
  const json = readFileSync(join(DIR, f), 'utf8');
  const svg = await page.evaluate(async (j) => {
    const utils = await import('https://esm.sh/@excalidraw/utils@0.1.2');
    const { exportToSvg } = utils.default;
    const data = JSON.parse(j);
    const el = await exportToSvg({
      elements: data.elements,
      appState: { ...data.appState, exportBackground: true, exportPadding: 24 },
      files: data.files || {},
    });
    return el.outerHTML;
  }, json);
  const out = f.replace('.excalidraw', '.svg');
  writeFileSync(join(DIR, out), svg);
  console.log(`exported ${out} (${svg.length} bytes)`);

  const pngDataUrl = await page.evaluate(async (j) => {
    const utils = await import('https://esm.sh/@excalidraw/utils@0.1.2');
    const { exportToBlob } = utils.default;
    const data = JSON.parse(j);
    const blob = await exportToBlob({
      elements: data.elements,
      appState: { ...data.appState, exportBackground: true, exportPadding: 24 },
      files: data.files || {},
      mimeType: 'image/png',
      getDimensions: (w, h) => ({ width: w * 2, height: h * 2, scale: 2 }),
    });
    return await new Promise((res) => {
      const r = new FileReader();
      r.onloadend = () => res(r.result);
      r.readAsDataURL(blob);
    });
  }, json);
  const pngOut = f.replace('.excalidraw', '.png');
  writeFileSync(join(DIR, pngOut),
    Buffer.from(pngDataUrl.split(',')[1], 'base64'));
  console.log(`exported ${pngOut}`);
}

await browser.close();
