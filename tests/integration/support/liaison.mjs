/** Run the ordinary stateless liaison CLI against the live Host plugin. */

import { execFile } from 'node:child_process'
import { join } from 'node:path'
import { REPO_ROOT } from './upstream.mjs'

const BIN = join(REPO_ROOT, 'src/bin.ts')

/**
 * One CLI invocation.
 * @param {object} input
 * @param {string[]} input.args - command and flags.
 * @param {Record<string, string>} input.env - exact Host environment.
 * @param {number} [input.timeoutMs] - process bound.
 * @returns {Promise<{value: unknown, stderr: string}>} parsed JSON or a thrown error.
 */
export async function runLiaison({ args, env, timeoutMs = 60_000 }) {
  const result = await new Promise((resolve, reject) => {
    execFile(process.execPath, [BIN, ...args], {
      cwd: REPO_ROOT,
      env: { ...process.env, ...env },
      timeout: timeoutMs,
      maxBuffer: 16 * 1024 * 1024,
    }, (error, stdout, stderr) => {
      if (error !== null && error.killed) {
        reject(new Error(`liaison ${args.join(' ')} timed out after ${String(timeoutMs)} ms\n${stderr}`))
        return
      }
      resolve({ code: error?.code ?? 0, stdout, stderr })
    })
  })
  if (result.code !== 0) {
    let detail = result.stderr.trim()
    try {
      detail = JSON.stringify(JSON.parse(detail), null, 2)
    } catch {
      // Keep the raw stderr when the CLI did not print one JSON document.
    }
    throw new Error(`liaison ${args.join(' ')} failed (${String(result.code)}):\n${detail}`)
  }
  return { value: JSON.parse(result.stdout), stderr: result.stderr }
}

/** Wait until the plugin socket answers `status`, then return that document. */
export async function waitForPlugin({ env, timeoutMs = 30_000 }) {
  const deadline = Date.now() + timeoutMs
  let last
  while (Date.now() < deadline) {
    try {
      return (await runLiaison({ args: ['status'], env })).value
    } catch (error) {
      last = error
      await new Promise(resolve => setTimeout(resolve, 100))
    }
  }
  throw new Error(`liaison plugin did not become reachable: ${last instanceof Error ? last.message : String(last)}`)
}
