#!/usr/bin/env node
/** Import every built public upstream entry in a fresh process, so no ESM failure is cached. */

import { createRequire } from 'node:module'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'
import { UPSTREAM_ENTRIES } from '../src/upstream-inventory.mjs'

const root = process.argv[2]
if (root === undefined) throw new Error('usage: verify-upstream-imports.mjs <upstream-root>')

for (const entry of UPSTREAM_ENTRIES) {
  try {
    const resolved = createRequire(join(root, entry.anchor)).resolve(entry.specifier)
    await import(pathToFileURL(resolved).href)
    console.log(`ok ${entry.key} ${entry.specifier}`)
  } catch (error) {
    console.error(`failed ${entry.key} ${entry.specifier}`)
    throw error
  }
}
