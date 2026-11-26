/** Bounded runtime probe for the pinned Chromium headless shell; no package-manager writes. */

import { spawnSync } from 'node:child_process'
import { join } from 'node:path'

export function playwrightRuntimeError(detail) {
  const trimmed = String(detail).trim()
  return new Error([
    'bootstrap: pinned Chromium headless shell failed to launch.',
    'Install the missing OS runtime libraries with your distribution package manager; bootstrap never runs sudo or modifies packages.',
    trimmed,
  ].filter(line => line !== '').join('\n'))
}

export function probePlaywrightRuntime(root, { run = spawnSync } = {}) {
  const probe = join(import.meta.dirname, 'probe-playwright-runtime.mjs')
  const result = run(process.execPath, [probe, root], { encoding: 'utf8' })
  if (result.error !== undefined) throw playwrightRuntimeError(result.error.message)
  if (result.status !== 0) throw playwrightRuntimeError(`${result.stdout ?? ''}${result.stderr ?? ''}`)
  return result
}
