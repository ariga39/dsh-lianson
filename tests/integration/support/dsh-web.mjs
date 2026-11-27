/** Launch a real `dsh web --no-open --patch` Host for integration tests. */

import { spawn } from 'node:child_process'
import { mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import { REPO_ROOT } from './upstream.mjs'

const BIN = join(REPO_ROOT, 'deepseek-harness/apps/cli/lib/bin.js')

function redact(output) {
  return output.replace(/([?&]token=)[^\s)]+/gu, '$1<redacted>')
}

/**
 * Start one real Host process.
 * @param {object} input
 * @param {string} input.root - scratch root outside the shared temp area.
 * @param {string} input.cwd - Host working directory.
 * @param {string} input.dshHome - exact Harness home.
 * @param {string} [input.runtimeDir] - liaison runtime override.
 * @param {string[]} [input.patches] - overlay patch files in argv order.
 * @param {Record<string, string>} [input.env] - extra environment.
 * @returns {Promise<{child: import('node:child_process').ChildProcess, launchUrl: string, pid: number, startupMs: number, output: () => string, close: () => Promise<void>, kill: (signal?: NodeJS.Signals) => Promise<void>}>}
 */
export async function startDshWeb({ root, cwd, dshHome, runtimeDir, patches = [], env: extraEnv = {} }) {
  await mkdir(dshHome, { recursive: true })
  const env = Object.fromEntries(Object.entries(process.env).filter(([name]) =>
    !/(?:KEY|SECRET|TOKEN|PASSWORD)/iu.test(name)))
  Object.assign(env, {
    DSH_HOME: dshHome,
    DSH_AGENTS_HOME: join(root, 'agents'),
    DSH_TELEMETRY_DISABLED: '1',
    DEEPSEEK_API_KEY: 'dsh-mock-key',
    NODE_NO_WARNINGS: '1',
    SSH_CONNECTION: '',
    SSH_TTY: '',
    ...(runtimeDir === undefined ? {} : { DSH_LIAISON_RUNTIME_DIR: runtimeDir }),
    ...extraEnv,
  })

  const args = [
    BIN,
    '--profile', 'web',
    ...patches.flatMap(path => ['--patch', path]),
    '--no-open',
    '--host', '127.0.0.1',
    '--port', '0',
  ]
  const started = performance.now()
  const child = spawn(process.execPath, args, { cwd, env, stdio: ['ignore', 'pipe', 'pipe'] })

  let output = ''
  let launchUrl
  try {
    launchUrl = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`dsh web did not become ready:\n${redact(output)}`)), 120_000)
      const append = (chunk) => {
        output = `${output}${String(chunk)}`.slice(-100_000)
        const match = /dsh web: (http:\/\/[^\s]+)/u.exec(output)
        if (match?.[1] === undefined) return
        clearTimeout(timer)
        resolve(match[1])
      }
      child.stdout.on('data', append)
      child.stderr.on('data', append)
      child.once('error', (error) => { clearTimeout(timer); reject(error) })
      child.once('exit', (code) => {
        clearTimeout(timer)
        reject(new Error(`dsh web exited before readiness (${String(code)}):\n${redact(output)}`))
      })
    })
  } catch (error) {
    child.kill('SIGKILL')
    throw error
  }

  const closed = new Promise((resolve) => child.once('exit', resolve))
  const kill = async (signal = 'SIGTERM') => {
    if (child.exitCode !== null) return
    child.kill(signal)
    if (signal !== 'SIGKILL') {
      const forced = setTimeout(() => child.kill('SIGKILL'), 10_000)
      forced.unref()
      await closed
      clearTimeout(forced)
    } else {
      await closed
    }
  }

  return {
    child,
    launchUrl,
    pid: child.pid,
    startupMs: Math.round(performance.now() - started),
    output: () => output,
    kill,
    close: () => kill('SIGTERM'),
  }
}
