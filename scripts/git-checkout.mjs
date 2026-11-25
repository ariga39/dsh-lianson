/** Detached checkout that cannot execute the caller's global hooks. */

import { spawnSync } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { dirname, join } from 'node:path'

function defaultRun(command, args) {
  const result = spawnSync(command, args, { stdio: 'inherit' })
  if (result.error !== undefined) throw result.error
  if (result.status !== 0) throw new Error(`${command} ${args.join(' ')} exited ${String(result.status)}`)
}

export function checkoutDetachedWithoutHooks(staging, ref, run = defaultRun) {
  const hooksDir = mkdtempSync(join(dirname(staging), '.bootstrap-empty-hooks-'))
  try {
    run('git', ['-c', `core.hooksPath=${hooksDir}`, '-C', staging, 'checkout', '--detach', ref])
  } finally {
    rmSync(hooksDir, { recursive: true, force: true })
  }
}
