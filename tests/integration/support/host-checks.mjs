/** Host-level observations used by the integration suite. */

import { execFile } from 'node:child_process'
import { stat } from 'node:fs/promises'
import { join } from 'node:path'
import { REPO_ROOT } from './upstream.mjs'

/**
 * Poll a predicate until it is true or the deadline expires.
 * @param {() => boolean | Promise<boolean>} predicate
 * @param {number} timeoutMs
 * @param {string} label
 */
export async function waitFor(predicate, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await predicate()) return
    await new Promise(resolve => setTimeout(resolve, 100))
  }
  throw new Error(`timed out waiting for ${label}`)
}

/** @returns the socket mode and the runtime directory mode. */
export async function socketModes(socketPath) {
  const socket = await stat(socketPath)
  const directory = await stat(join(socketPath, '..'))
  return {
    socket: (socket.mode & 0o777).toString(8).padStart(3, '0'),
    directory: (directory.mode & 0o777).toString(8).padStart(3, '0'),
  }
}

/** @returns TCP ports listening in one process. */
export async function listeningTcpPorts(pid) {
  const output = await new Promise((resolve, reject) => {
    execFile('ss', ['-ltnp'], (error, stdout) => {
      if (error === null) resolve(stdout)
      else reject(error)
    })
  })
  const ports = new Set()
  for (const line of output.split('\n')) {
    if (!line.includes(`pid=${String(pid)},`)) continue
    const match = /\s(?:\[[^\]]+\]|[^:\s]+):(\d+)\s/u.exec(line)
    if (match?.[1] !== undefined) ports.add(Number(match[1]))
  }
  return [...ports].sort((left, right) => left - right)
}

/**
 * Start the real liaison composition in a child process with the same runtime
 * directory, and expect its single-owner lock to refuse the second socket.
 * @param {Record<string, string>} env
 */
export async function expectSocketInUse(env) {
  const script = `
    import { startLiaison } from ${JSON.stringify(join(REPO_ROOT, 'src/start.ts'))}
    try {
      const runtime = await startLiaison({
        env: process.env,
        origin: () => undefined,
        collectEntries: () => [],
        sandboxPolicy: () => undefined,
        logger: { warn: () => {} },
      })
      await runtime.dispose()
      console.log('SOCKET_ERROR:none')
    } catch (error) {
      console.log('SOCKET_ERROR:' + (error && error.code ? error.code : String(error)))
    }
  `
  const output = await new Promise((resolve, reject) => {
    execFile(process.execPath, ['--input-type=module', '-e', script], {
      cwd: REPO_ROOT, env: { ...process.env, ...env }, timeout: 30_000,
    }, (error, stdout, stderr) => {
      if (error !== null) reject(new Error(`second owner failed unexpectedly: ${stderr || error.message}`))
      else resolve(stdout)
    })
  })
  if (!output.includes('SOCKET_ERROR:socket-in-use')) {
    throw new Error(`second owner was not refused: ${output.trim()}`)
  }
}
