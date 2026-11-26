#!/usr/bin/env node
/** Launch and close the pinned Chromium headless shell once, proving its OS runtime. */

import { createRequire } from 'node:module'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'

const root = process.argv[2]
if (root === undefined) throw new Error('usage: probe-playwright-runtime.mjs <upstream-root>')

const resolved = createRequire(join(root, 'apps/web/package.json')).resolve('playwright')
const playwright = await import(pathToFileURL(resolved).href)
const chromium = (playwright.default ?? playwright).chromium

let browser
try {
  browser = await chromium.launch({ timeout: 30_000 })
} finally {
  if (browser !== undefined) await browser.close().catch(() => {})
}
console.log('playwright headless shell launch ok')
