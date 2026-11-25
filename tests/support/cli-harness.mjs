import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { readdir, readFile, readlink } from 'node:fs/promises'
import { join } from 'node:path'

export const CLI_BIN = join(import.meta.dirname, '../../bin/dsh-liaison.mjs')

export function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

export async function waitFor(check, { timeoutMs = 30_000, intervalMs = 50, label = 'condition' } = {}) {
  const deadline = Date.now() + timeoutMs
  let lastError
  while (Date.now() < deadline) {
    try {
      const value = await check()
      if (value) return value
    } catch (error) {
      lastError = error
    }
    await sleep(intervalMs)
  }
  const suffix = lastError === undefined ? '' : `: ${lastError instanceof Error ? lastError.message : String(lastError)}`
  throw new Error(`timed out waiting for ${label}${suffix}`)
}

export async function waitForExit(pid, timeoutMs = 10_000) {
  return waitFor(() => {
    try {
      process.kill(pid, 0)
      return false
    } catch (error) {
      return error?.code === 'ESRCH'
    }
  }, { timeoutMs, intervalMs: 50, label: `pid ${String(pid)} to exit` })
}

export async function runCli(args, { cwd, env, timeoutMs = 180_000 } = {}) {
  const child = spawn(process.execPath, [CLI_BIN, ...args], {
    cwd,
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  let stdout = ''
  let stderr = ''
  child.stdout.setEncoding('utf8')
  child.stderr.setEncoding('utf8')
  child.stdout.on('data', chunk => { stdout += chunk })
  child.stderr.on('data', chunk => { stderr += chunk })

  const result = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      child.kill('SIGKILL')
      reject(new Error(`dsh-liaison ${args.join(' ')} timed out\nstdout:\n${stdout}\nstderr:\n${stderr}`))
    }, timeoutMs)
    child.once('error', error => { clearTimeout(timer); reject(error) })
    child.once('exit', (code, signal) => {
      clearTimeout(timer)
      resolve({ code, signal, stdout, stderr })
    })
  })

  let envelope
  const lines = stdout.trim().split('\n').filter(Boolean)
  if (lines.length > 0) {
    try { envelope = JSON.parse(lines.at(-1)) } catch { envelope = undefined }
  }
  return { ...result, envelope }
}

export async function rawRpcStatus(origin, cookie, method = 'session/modelCatalog') {
  const response = await fetch(new URL(`/api/${method}`, origin), {
    method: 'POST',
    redirect: 'manual',
    headers: {
      accept: 'application/json',
      'content-type': 'application/json',
      ...cookie === undefined ? {} : { cookie },
    },
    body: JSON.stringify({
      type: 'client-request',
      rpcId: randomUUID(),
      method,
      payload: { args: {} },
    }),
  })
  await response.arrayBuffer().catch(() => {})
  return response.status
}

export async function socketInodes(pid) {
  const fds = await readdir(`/proc/${String(pid)}/fd`)
  const inodes = new Set()
  for (const fd of fds) {
    try {
      const target = await readlink(`/proc/${String(pid)}/fd/${fd}`)
      const match = /^socket:\[(\d+)\]$/u.exec(target)
      if (match?.[1] !== undefined) inodes.add(match[1])
    } catch {
      // The process may close a descriptor while the directory is read.
    }
  }
  return inodes
}

function parseProcNet(content) {
  const rows = []
  for (const line of content.trim().split('\n').slice(1)) {
    const fields = line.trim().split(/\s+/u)
    if (fields.length < 10) continue
    rows.push({ local: fields[1], state: fields[3], inode: fields[9] })
  }
  return rows
}

/** Return every TCP socket owned by `pid` that is in the LISTEN state. */
export async function listeningTcpSockets(pid) {
  const inodes = await socketInodes(pid)
  if (inodes.size === 0) return []
  const listening = []
  for (const file of ['/proc/net/tcp', '/proc/net/tcp6']) {
    let content
    try { content = await readFile(file, 'utf8') } catch { continue }
    for (const row of parseProcNet(content)) {
      if (row.state === '0A' && inodes.has(row.inode)) listening.push({ file, ...row })
    }
  }
  return listening
}
