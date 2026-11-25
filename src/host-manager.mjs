/** Managed `dsh web` Host lifecycle: private state, attach-or-spawn, and verified shutdown. */

import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { chmod, link, lstat, mkdir, open, readFile, rename, stat, unlink } from 'node:fs/promises'
import { homedir } from 'node:os'
import { basename, dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { LiaisonError } from './errors.mjs'
import { redactSecrets } from './redaction.mjs'

export const REPO_ROOT = fileURLToPath(new URL('..', import.meta.url))
export const DSH_BIN = join(REPO_ROOT, 'deepseek-harness/apps/cli/lib/bin.js')
export const HOST_MARKER_ENV = 'DSH_LIAISON_HOST_MARKER'
const RECORD_VERSION = 1
const LOCK_VERSION = 1

function isRecord(value) {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function sleep(ms) {
  return new Promise(resolvePromise => setTimeout(resolvePromise, ms))
}

/** State directory: explicit test override, XDG runtime, then the user's state home. */
export function resolveStateDir(env = process.env) {
  if (typeof env.DSH_LIAISON_STATE_DIR === 'string' && env.DSH_LIAISON_STATE_DIR !== '') {
    return resolve(env.DSH_LIAISON_STATE_DIR)
  }
  if (typeof env.XDG_RUNTIME_DIR === 'string' && env.XDG_RUNTIME_DIR !== '') {
    return join(env.XDG_RUNTIME_DIR, 'dsh-liaison')
  }
  return join(env.HOME ?? homedir(), '.local', 'state', 'dsh-liaison')
}

/** The loopback authorities a managed Host may use; no DNS or remote origin is accepted. */
export function isLoopbackHostname(hostname) {
  const value = hostname.replace(/^\[|\]$/gu, '').toLowerCase()
  if (value === 'localhost' || value === '::1') return true
  return /^127(?:\.\d{1,3}){3}$/u.test(value)
}

/**
 * Normalize and validate an HTTP origin that may receive credentials.
 * @param {unknown} origin
 * @returns {string}
 */
export function assertLoopbackOrigin(origin) {
  let url
  try { url = new URL(String(origin)) } catch {
    throw new LiaisonError('HOST_ORIGIN_UNSAFE', 'Host origin is not a valid URL')
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new LiaisonError('HOST_ORIGIN_UNSAFE', 'Host origin must use HTTP or HTTPS')
  }
  if (url.username !== '' || url.password !== '') {
    throw new LiaisonError('HOST_ORIGIN_UNSAFE', 'Host origin must not carry user credentials')
  }
  if (!isLoopbackHostname(url.hostname)) {
    throw new LiaisonError('HOST_ORIGIN_UNSAFE', 'Host origin must be loopback')
  }
  if (url.pathname !== '/' || url.search !== '' || url.hash !== '') {
    throw new LiaisonError('HOST_ORIGIN_UNSAFE', 'Host origin must not contain a path or query')
  }
  return url.origin
}

/** Create the state directory and force user-only access regardless of umask. */
export async function ensurePrivateDir(directory) {
  await mkdir(directory, { recursive: true, mode: 0o700 })
  const info = await stat(directory)
  if (!info.isDirectory()) throw new LiaisonError('STATE_DIR_INVALID', 'State path is not a directory')
  await chmod(directory, 0o700)
}

async function readJsonFile(path) {
  let raw
  try { raw = await readFile(path, 'utf8') } catch (error) {
    if (error?.code === 'ENOENT') return null
    throw error
  }
  try { return { raw, value: JSON.parse(raw) } } catch (error) {
    return { raw, invalid: true, error }
  }
}

async function writePrivateAtomic(path, value) {
  const temporary = join(dirname(path), `.${basename(path)}.${String(process.pid)}.${randomUUID()}.tmp`)
  const handle = await open(temporary, 'wx', 0o600)
  try {
    await handle.writeFile(`${JSON.stringify(value, null, 2)}\n`)
    await handle.sync()
    await handle.close()
    await rename(temporary, path)
    await chmod(path, 0o600)
  } catch (error) {
    await handle.close().catch(() => {})
    await unlink(temporary).catch(() => {})
    throw error
  }
}

async function removeIfUnchanged(path, raw) {
  try {
    const current = await readFile(path, 'utf8')
    if (current === raw) await unlink(path)
    return current === raw
  } catch (error) {
    if (error?.code === 'ENOENT') return true
    return false
  }
}

let cachedBootId
async function bootId() {
  if (cachedBootId !== undefined) return cachedBootId
  try { cachedBootId = (await readFile('/proc/sys/kernel/random/boot_id', 'utf8')).trim() } catch {
    cachedBootId = 'unknown'
  }
  return cachedBootId
}

async function readProcStat(pid) {
  try {
    const content = await readFile(`/proc/${String(pid)}/stat`, 'utf8')
    const end = content.lastIndexOf(')')
    if (end === -1) return undefined
    const fields = content.slice(end + 2).trim().split(/\s+/u)
    return { state: fields[0], startTime: fields[19] }
  } catch (error) {
    if (error?.code === 'ENOENT') return undefined
    throw error
  }
}

/** Stable process identity: boot id plus /proc start time, resistant to PID reuse. */
export async function processStartToken(pid) {
  const info = await readProcStat(pid)
  if (info?.startTime === undefined) return undefined
  return `${await bootId()}:${info.startTime}`
}

async function readProcessEnvironment(pid) {
  try { return await readFile(`/proc/${String(pid)}/environ`, 'utf8') } catch { return undefined }
}

/**
 * Verify that a record's pid is still the exact process we spawned.
 * @param {{pid: number, marker?: string, procStart?: string}} record
 */
export async function inspectProcess(record) {
  if (!Number.isInteger(record?.pid) || record.pid <= 0) {
    return { ok: false, alive: false, reason: 'dead' }
  }
  let info
  try { info = await readProcStat(record.pid) } catch {
    return { ok: false, alive: false, reason: 'unverified' }
  }
  if (info === undefined || info.state === 'Z') return { ok: false, alive: false, reason: 'dead' }
  const current = await processStartToken(record.pid)
  if (record.procStart !== undefined && current !== undefined && record.procStart !== current) {
    return { ok: false, alive: true, reason: 'reused' }
  }
  if (typeof record.marker !== 'string' || record.marker === '') {
    return { ok: false, alive: true, reason: 'unverified' }
  }
  const environment = await readProcessEnvironment(record.pid)
  if (environment === undefined) return { ok: false, alive: true, reason: 'unverified' }
  const expected = `${HOST_MARKER_ENV}=${record.marker}`
  if (!environment.split('\0').includes(expected)) {
    return { ok: false, alive: true, reason: 'unverified' }
  }
  return { ok: true, alive: true, reason: 'verified', procStart: current }
}

function validateHostRecord(value) {
  if (!isRecord(value)) throw new LiaisonError('HOST_RECORD_INVALID', 'Host record is not an object')
  const pid = Number(value.pid)
  if (!Number.isInteger(pid) || pid <= 0) throw new LiaisonError('HOST_RECORD_INVALID', 'Host record has no valid pid')
  const origin = assertLoopbackOrigin(value.origin)
  const port = Number(value.port ?? new URL(origin).port)
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new LiaisonError('HOST_RECORD_INVALID', 'Host record has no valid port')
  }
  const launchToken = value.launchToken
  if (typeof launchToken !== 'string' || launchToken.length < 16) {
    throw new LiaisonError('HOST_RECORD_INVALID', 'Host record has no launch token')
  }
  const dshHome = typeof value.dshHome === 'string' ? resolve(value.dshHome) : undefined
  if (dshHome === undefined) throw new LiaisonError('HOST_RECORD_INVALID', 'Host record has no dshHome')
  return {
    ...value,
    version: RECORD_VERSION,
    pid,
    origin,
    port,
    launchToken,
    dshHome,
    startedAt: typeof value.startedAt === 'string' ? value.startedAt : new Date().toISOString(),
    marker: typeof value.marker === 'string' ? value.marker : undefined,
    procStart: typeof value.procStart === 'string' ? value.procStart : undefined,
  }
}

/** One atomically-created lock file with stale-PID recovery. */
export class FileLock {
  constructor(path) {
    this.path = path
    this.content = undefined
  }

  async acquire({ timeoutMs = 30_000 } = {}) {
    const started = Date.now()
    const identity = {
      version: LOCK_VERSION,
      pid: process.pid,
      procStart: await processStartToken(process.pid),
      createdAt: new Date().toISOString(),
    }
    const content = `${JSON.stringify(identity)}\n`
    while (true) {
      const temporary = join(dirname(this.path),
        `.${basename(this.path)}.${String(process.pid)}.${randomUUID()}.tmp`)
      let created = false
      try {
        // Build the complete owner record before publishing the lock name, then
        // publish it with the OS hard-link primitive. A contender can never see
        // an empty lock owned by a live writer.
        const handle = await open(temporary, 'wx', 0o600)
        try {
          await handle.writeFile(content)
          await handle.sync()
        } finally {
          await handle.close()
        }
        try {
          await link(temporary, this.path)
          created = true
        } catch (error) {
          if (error?.code !== 'EEXIST') throw error
        }
      } catch (error) {
        if (error?.code !== 'EEXIST') throw error
      } finally {
        await unlink(temporary).catch(() => {})
      }
      if (created) {
        this.content = content
        return () => this.release()
      }
      if (await this.#clearStale()) continue
      if (Date.now() - started >= timeoutMs) {
        throw new LiaisonError('HOST_LOCK_TIMEOUT', 'Timed out waiting for the Host lock')
      }
      await sleep(25)
    }
  }

  async release() {
    if (this.content === undefined) return
    const content = this.content
    this.content = undefined
    await removeIfUnchanged(this.path, content).catch(() => {})
  }

  async #clearStale() {
    let snapshot
    let raw
    try {
      snapshot = await lstat(this.path)
      raw = await readFile(this.path, 'utf8')
    } catch (error) {
      return error?.code === 'ENOENT'
    }
    let parsed
    try { parsed = JSON.parse(raw) } catch { parsed = undefined }
    const incomplete = parsed === undefined
      || !Number.isInteger(parsed.pid)
      || parsed.pid <= 0
    // Atomic hard-link publication never exposes a partial record. A legacy or
    // externally created empty record gets a short grace period so a concurrent
    // writer is never mistaken for a dead holder; after that it is reclaimable.
    const stale = incomplete
      ? Date.now() - snapshot.mtimeMs > 10_000
      : await this.#ownerGone(parsed)
    if (!stale) return false
    try {
      const current = await lstat(this.path)
      if (current.ino !== snapshot.ino) return true
      if (await readFile(this.path, 'utf8') !== raw) return true
      await unlink(this.path)
      return true
    } catch (error) {
      return error?.code === 'ENOENT'
    }
  }

  async #ownerGone(owner) {
    const info = await readProcStat(owner.pid)
    if (info === undefined || info.state === 'Z') return true
    if (typeof owner.procStart === 'string') {
      const current = await processStartToken(owner.pid)
      if (current !== undefined && current !== owner.procStart) return true
    }
    return false
  }
}

async function probeOrigin(origin, timeoutMs) {
  const url = new URL('/', assertLoopbackOrigin(origin))
  try {
    const response = await fetch(url, {
      redirect: 'manual',
      signal: AbortSignal.timeout(timeoutMs),
      headers: { accept: 'text/html' },
    })
    await response.arrayBuffer().catch(() => {})
    return response.status === 200 || response.status === 401
  } catch {
    return false
  }
}

async function openUnlinkedCapture(stateDir, name) {
  const path = join(stateDir, `.${name}-${randomUUID()}.log`)
  const handle = await open(path, 'a+', 0o600)
  await chmod(path, 0o600)
  return { path, handle }
}

async function readCaptureTail(handle, position) {
  const buffer = Buffer.allocUnsafe(65_536)
  const { bytesRead } = await handle.read(buffer, 0, buffer.length, position)
  if (bytesRead === 0) return { text: '', position }
  return { text: buffer.subarray(0, bytesRead).toString('utf8'), position: position + bytesRead }
}

/** A managed Host handle. `launchToken` is a credential and must never be serialized to IPC. */
function hostHandle(record) {
  return {
    pid: record.pid,
    origin: record.origin,
    port: record.port,
    launchToken: record.launchToken,
    dshHome: record.dshHome,
    startedAt: record.startedAt,
    marker: record.marker,
    procStart: record.procStart,
    managed: true,
  }
}

export class HostManager {
  constructor({
    stateDir = resolveStateDir(),
    env = process.env,
    binPath = DSH_BIN,
    dshHome,
    cwd,
    patches = [],
    port = 0,
    probeTimeoutMs = 5_000,
    spawnTimeoutMs = 120_000,
  } = {}) {
    this.stateDir = resolve(stateDir)
    this.env = env
    this.binPath = binPath
    this.defaultDshHome = dshHome
    this.defaultCwd = cwd
    this.defaultPatches = patches
    this.defaultPort = port
    this.probeTimeoutMs = probeTimeoutMs
    this.spawnTimeoutMs = spawnTimeoutMs
    this.recordPath = join(this.stateDir, 'host.json')
    this.lock = new FileLock(join(this.stateDir, 'host.lock'))
  }

  async #loadRecord() {
    const file = await readJsonFile(this.recordPath)
    if (file === null) return null
    if (file.invalid === true) return { invalid: true, raw: file.raw, reason: 'invalid-json' }
    try {
      return { record: validateHostRecord(file.value), raw: file.raw }
    } catch (error) {
      if (error?.code === 'HOST_ORIGIN_UNSAFE') throw error
      return { invalid: true, raw: file.raw, reason: error?.code ?? 'invalid-record' }
    }
  }

  /** Read and verify the current managed Host without spawning or probing. */
  async readOwnedHost() {
    const loaded = await this.#loadRecord()
    if (loaded === null || loaded.invalid === true) return null
    const verification = await inspectProcess(loaded.record)
    if (!verification.ok) {
      throw new LiaisonError('HOST_RECORD_UNOWNED', `Host record is ${verification.reason}`)
    }
    return hostHandle(loaded.record)
  }

  /** Read-only status: record validity, ownership, and a loopback HTTP probe. */
  async inspectHost({ probe = true, timeoutMs = 1_000 } = {}) {
    const loaded = await this.#loadRecord()
    if (loaded === null) return null
    if (loaded.invalid === true) return { verified: false, reason: loaded.reason }
    const verification = await inspectProcess(loaded.record)
    const reachable = probe && verification.ok ? await probeOrigin(loaded.record.origin, timeoutMs) : false
    return {
      pid: loaded.record.pid,
      origin: loaded.record.origin,
      port: loaded.record.port,
      dshHome: loaded.record.dshHome,
      startedAt: loaded.record.startedAt,
      verified: verification.ok,
      reachable,
      reason: verification.reason,
    }
  }

  /** Attach to a verified live Host, reclaim a stale record, or spawn exactly one Host. */
  async ensureHost(options = {}) {
    await ensurePrivateDir(this.stateDir)
    const release = await this.lock.acquire()
    try {
      const loaded = await this.#loadRecord()
      if (loaded !== null) {
        if (loaded.invalid === true) {
          await removeIfUnchanged(this.recordPath, loaded.raw)
        } else {
          const verification = await inspectProcess(loaded.record)
          if (verification.ok) {
            const reachable = await this.#probeWithRetry(loaded.record.origin, this.probeTimeoutMs)
            if (reachable) return hostHandle(loaded.record)
            throw new LiaisonError('HOST_UNRESPONSIVE',
              'A verified managed Host is alive but its loopback endpoint is not answering')
          }
          if (verification.reason === 'dead' || verification.reason === 'reused') {
            await removeIfUnchanged(this.recordPath, loaded.raw)
          } else {
            throw new LiaisonError('HOST_RECORD_UNOWNED',
              `Refusing to take over a Host record owned by an unverified process (${verification.reason})`)
          }
        }
      }
      return await this.#spawnHost(options)
    } finally {
      await release()
    }
  }

  async #probeWithRetry(origin, timeoutMs) {
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
      if (await probeOrigin(origin, Math.max(250, Math.min(2_000, deadline - Date.now())))) return true
      await sleep(100)
    }
    return false
  }

  async #spawnHost(options) {
    const dshHome = resolve(options.dshHome ?? this.defaultDshHome ?? this.env.DSH_HOME ?? join(homedir(), '.dsh'))
    const cwd = resolve(options.cwd ?? this.defaultCwd ?? process.cwd())
    const patches = (options.patches ?? this.defaultPatches).map(path => resolve(path))
    const port = Number(options.port ?? this.defaultPort)
    if (!Number.isInteger(port) || port < 0 || port > 65_535) {
      throw new LiaisonError('HOST_PORT_INVALID', 'Host port must be an integer from 0 to 65535')
    }

    const marker = randomUUID()
    const env = {
      ...process.env,
      ...this.env,
      ...options.env,
      DSH_HOME: dshHome,
      [HOST_MARKER_ENV]: marker,
    }
    const args = [this.binPath, 'web']
    for (const patch of patches) args.push('--patch', patch)
    args.push('--no-open', '--host', '127.0.0.1', '--port', String(port))

    const stdout = await openUnlinkedCapture(this.stateDir, 'host-stdout')
    const stderr = await openUnlinkedCapture(this.stateDir, 'host-stderr')
    const child = spawn(process.execPath, args, {
      cwd,
      env,
      stdio: ['ignore', stdout.handle.fd, stderr.handle.fd],
    })
    await unlink(stdout.path).catch(() => {})
    await unlink(stderr.path).catch(() => {})

    let output = ''
    let errorOutput = ''
    let stdoutPosition = 0
    let stderrPosition = 0
    let launchUrl
    let spawnError
    child.once('error', error => { spawnError = error })
    const started = Date.now()
    try {
      while (Date.now() - started < this.spawnTimeoutMs) {
        if (spawnError !== undefined) {
          throw new LiaisonError('HOST_SPAWN_FAILED',
            `Could not start dsh web: ${redactSecrets(spawnError.message)}`)
        }
        if (stdoutPosition >= 0) {
          const read = await readCaptureTail(stdout.handle, stdoutPosition)
          stdoutPosition = read.position
          output = `${output}${read.text}`.slice(-100_000)
        }
        const read = await readCaptureTail(stderr.handle, stderrPosition)
        stderrPosition = read.position
        errorOutput = `${errorOutput}${read.text}`.slice(-100_000)
        const match = /dsh web: (http:\/\/[^\s]+)/u.exec(`${output}${errorOutput}`)
        if (match?.[1] !== undefined) {
          launchUrl = match[1]
          break
        }
        if (child.exitCode !== null) {
          throw new LiaisonError('HOST_SPAWN_FAILED',
            `dsh web exited before readiness (${String(child.exitCode)}): ${redactSecrets(`${output}${errorOutput}`)}`)
        }
        await sleep(25)
      }
      if (launchUrl === undefined) {
        throw new LiaisonError('HOST_SPAWN_TIMEOUT',
          `dsh web did not become ready: ${redactSecrets(`${output}${errorOutput}`)}`)
      }

      let url
      try { url = new URL(launchUrl) } catch {
        throw new LiaisonError('HOST_LAUNCH_URL_INVALID', 'dsh web printed an invalid launch URL')
      }
      const origin = assertLoopbackOrigin(url.origin)
      const launchToken = url.searchParams.get('token')
      if (launchToken === null || launchToken.length < 16) {
        throw new LiaisonError('HOST_LAUNCH_URL_INVALID', 'dsh web launch URL has no token')
      }
      if (child.pid === undefined) {
        throw new LiaisonError('HOST_SPAWN_FAILED', 'dsh web did not provide a pid')
      }
      if (child.exitCode !== null) {
        throw new LiaisonError('HOST_SPAWN_FAILED', 'dsh web exited immediately after readiness')
      }

      const record = {
        version: RECORD_VERSION,
        pid: child.pid,
        origin,
        port: Number(url.port),
        launchToken,
        dshHome,
        startedAt: new Date().toISOString(),
        marker,
        procStart: await processStartToken(child.pid),
        cwd,
      }
      await writePrivateAtomic(this.recordPath, record)
      return hostHandle(record)
    } catch (error) {
      if (child.exitCode === null) child.kill('SIGTERM')
      throw error
    } finally {
      await stdout.handle.close().catch(() => {})
      await stderr.handle.close().catch(() => {})
    }
  }

  /**
   * Stop only a Host whose process identity is still the exact one recorded.
   * A dead, reused, or unverified pid receives no signal.
   */
  async stopHost({ timeoutMs = 10_000, killTimeoutMs = 2_000 } = {}) {
    await ensurePrivateDir(this.stateDir)
    const release = await this.lock.acquire()
    try {
      const loaded = await this.#loadRecord()
      if (loaded === null || loaded.invalid === true) {
        if (loaded?.invalid === true) await removeIfUnchanged(this.recordPath, loaded.raw)
        return { stopped: false, reason: 'missing', pid: null }
      }
      const { record, raw } = loaded
      const verification = await inspectProcess(record)
      if (!verification.ok) {
        if (verification.reason === 'dead' || verification.reason === 'reused') {
          await removeIfUnchanged(this.recordPath, raw)
        }
        return { stopped: false, reason: verification.reason, pid: record.pid }
      }

      try { process.kill(record.pid, 'SIGTERM') } catch (error) {
        if (error?.code !== 'ESRCH') throw error
      }
      const deadline = Date.now() + timeoutMs
      while (Date.now() < deadline) {
        const current = await inspectProcess(record)
        if (!current.ok) {
          await removeIfUnchanged(this.recordPath, raw)
          return { stopped: true, reason: 'exited', pid: record.pid, signal: 'SIGTERM' }
        }
        await sleep(100)
      }

      // Re-verify immediately before escalation so a recycled pid cannot be hit.
      const final = await inspectProcess(record)
      if (!final.ok) {
        await removeIfUnchanged(this.recordPath, raw)
        return { stopped: true, reason: 'exited', pid: record.pid, signal: 'SIGTERM' }
      }
      process.kill(record.pid, 'SIGKILL')
      const killDeadline = Date.now() + killTimeoutMs
      while (Date.now() < killDeadline) {
        if (!(await inspectProcess(record)).ok) break
        await sleep(50)
      }
      await removeIfUnchanged(this.recordPath, raw)
      return { stopped: true, reason: 'killed', pid: record.pid, signal: 'SIGKILL' }
    } finally {
      await release()
    }
  }
}
