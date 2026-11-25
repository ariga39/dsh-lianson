/** Managed `dsh web` Host lifecycle: private state, attach-or-spawn, and verified shutdown. */

import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { chmod, mkdir, open, readdir, readFile, rename, stat, unlink } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { homedir } from 'node:os'
import { basename, dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { LiaisonError } from './errors.mjs'
import { redactSecrets } from './redaction.mjs'

export const REPO_ROOT = fileURLToPath(new URL('..', import.meta.url))
export const DSH_BIN = join(REPO_ROOT, 'deepseek-harness/apps/cli/lib/bin.js')
const FLOCK_ANCHOR = join(REPO_ROOT, 'deepseek-harness/native/system/packages/entry/package.json')
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

const procfsAvailability = new Map()

async function procfsAvailable(procRoot = '/proc') {
  if (procfsAvailability.has(procRoot)) return procfsAvailability.get(procRoot)
  if (procRoot === '/proc' && process.platform !== 'linux') {
    procfsAvailability.set(procRoot, false)
    return false
  }
  try {
    await readFile(join(procRoot, 'self', 'stat'), 'utf8')
    procfsAvailability.set(procRoot, true)
    return true
  } catch {
    procfsAvailability.set(procRoot, false)
    return false
  }
}

/** Liveness without procfs; unknown means the pid may exist but identity is unprovable. */
export async function pidLiveness(pid) {
  try {
    process.kill(pid, 0)
    return 'alive'
  } catch (error) {
    if (error?.code === 'ESRCH') return 'dead'
    return 'unknown'
  }
}

async function readProcStat(pid, procRoot = '/proc') {
  try {
    const content = await readFile(join(procRoot, String(pid), 'stat'), 'utf8')
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
export async function processStartToken(pid, procRoot = '/proc') {
  const info = await readProcStat(pid, procRoot)
  if (info?.startTime === undefined) return undefined
  return `${await bootId()}:${info.startTime}`
}

async function readProcessEnvironment(pid, procRoot = '/proc') {
  try { return await readFile(join(procRoot, String(pid), 'environ'), 'utf8') } catch { return undefined }
}

/**
 * Find live processes carrying one launch marker. Only used for a write-ahead
 * record whose pid update was lost; multiple matches are deliberately ambiguous.
 */
async function findProcessesByMarker(marker, procRoot = '/proc') {
  if (!await procfsAvailable(procRoot)) return { ok: false, reason: 'unsupported', matches: [] }
  let names
  try { names = await readdir(procRoot) } catch { return { ok: false, reason: 'unsupported', matches: [] } }
  const matches = []
  for (const name of names) {
    if (!/^\d+$/u.test(name)) continue
    const pid = Number(name)
    if (pid <= 0) continue
    const environment = await readProcessEnvironment(pid, procRoot)
    if (environment === undefined) continue
    if (!environment.split('\0').includes(`${HOST_MARKER_ENV}=${marker}`)) continue
    const info = await readProcStat(pid, procRoot).catch(() => undefined)
    if (info === undefined || info.state === 'Z') continue
    matches.push({ pid, procStart: `${await bootId()}:${info.startTime}` })
  }
  return { ok: true, reason: 'found', matches }
}

/**
 * Verify that a record's pid is still the exact process we spawned.
 *
 * A missing or unreadable procfs proves nothing about the pid: liveness comes
 * from the OS probe, and identity remains unverified. Only a confirmed dead or
 * reused pid is treated as safe to reclaim.
 * @param {{pid: number, marker?: string, procStart?: string}} record
 * @param {{procRoot?: string}} [options]
 */
export async function inspectProcess(record, { procRoot = '/proc' } = {}) {
  if (!Number.isInteger(record?.pid) || record.pid <= 0) {
    return { ok: false, alive: false, reason: 'dead' }
  }
  const procfs = await procfsAvailable(procRoot)
  if (!procfs) {
    const liveness = await pidLiveness(record.pid)
    if (liveness === 'dead') return { ok: false, alive: false, reason: 'dead' }
    return { ok: false, alive: true, reason: 'unverified', unsupported: true }
  }

  let info
  try { info = await readProcStat(record.pid, procRoot) } catch {
    const liveness = await pidLiveness(record.pid)
    if (liveness === 'dead') return { ok: false, alive: false, reason: 'dead' }
    return { ok: false, alive: true, reason: 'unverified', hidden: true }
  }
  if (info === undefined) {
    const liveness = await pidLiveness(record.pid)
    if (liveness === 'dead') return { ok: false, alive: false, reason: 'dead' }
    return { ok: false, alive: true, reason: 'unverified', hidden: true }
  }
  if (info.state === 'Z') return { ok: false, alive: false, reason: 'dead' }
  const current = `${await bootId()}:${info.startTime}`
  if (record.procStart !== undefined && current !== undefined && record.procStart !== current) {
    return { ok: false, alive: true, reason: 'reused' }
  }
  if (typeof record.marker !== 'string' || record.marker === '') {
    return { ok: false, alive: true, reason: 'unverified' }
  }
  const environment = await readProcessEnvironment(record.pid, procRoot)
  if (environment === undefined) return { ok: false, alive: true, reason: 'unverified' }
  const expected = `${HOST_MARKER_ENV}=${record.marker}`
  if (!environment.split('\0').includes(expected)) {
    return { ok: false, alive: true, reason: 'unverified' }
  }
  return { ok: true, alive: true, reason: 'verified', procStart: current }
}

function validateHostRecord(value) {
  if (!isRecord(value)) throw new LiaisonError('HOST_RECORD_INVALID', 'Host record is not an object')
  const state = value.state
  if (state !== 'launching' && state !== 'starting' && state !== 'ready') {
    throw new LiaisonError('HOST_RECORD_INVALID', 'Host record has no valid state')
  }
  const marker = value.marker
  if (typeof marker !== 'string' || marker === '') {
    throw new LiaisonError('HOST_RECORD_INVALID', 'Host record has no launch marker')
  }
  const dshHome = typeof value.dshHome === 'string' ? resolve(value.dshHome) : undefined
  if (dshHome === undefined) throw new LiaisonError('HOST_RECORD_INVALID', 'Host record has no dshHome')

  const record = {
    ...value,
    version: RECORD_VERSION,
    state,
    marker,
    dshHome,
    cwd: typeof value.cwd === 'string' ? resolve(value.cwd) : undefined,
    requestedPort: Number.isInteger(value.requestedPort) ? value.requestedPort : undefined,
    startedAt: typeof value.startedAt === 'string' ? value.startedAt : new Date().toISOString(),
    pid: undefined,
    procStart: typeof value.procStart === 'string' ? value.procStart : undefined,
  }

  if (state === 'launching') return record

  const pid = Number(value.pid)
  if (!Number.isInteger(pid) || pid <= 0) throw new LiaisonError('HOST_RECORD_INVALID', 'Host record has no valid pid')
  record.pid = pid
  if (state === 'starting') return record

  const origin = assertLoopbackOrigin(value.origin)
  const port = Number(value.port ?? new URL(origin).port)
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new LiaisonError('HOST_RECORD_INVALID', 'Host record has no valid port')
  }
  const launchToken = value.launchToken
  if (typeof launchToken !== 'string' || launchToken.length < 16) {
    throw new LiaisonError('HOST_RECORD_INVALID', 'Host record has no launch token')
  }
  return { ...record, origin, port, launchToken }
}

let cachedFlock
async function loadFlock() {
  if (cachedFlock !== undefined) return cachedFlock
  try {
    const resolved = createRequire(FLOCK_ANCHOR).resolve('@deepseek-ai/node-addon-system/flock')
    cachedFlock = await import(pathToFileURL(resolved).href)
  } catch {
    cachedFlock = null
  }
  return cachedFlock
}

/** Kernel-released flock on a persistent lock file. The path is never unlinked. */
class FlockLock {
  constructor(path, flock) {
    this.path = path
    this.flock = flock
  }

  async acquire({ timeoutMs = 30_000 } = {}) {
    const handle = await open(this.path, 'a+', 0o600)
    await chmod(this.path, 0o600).catch(() => {})
    const started = Date.now()
    try {
      while (true) {
        try {
          await this.flock.tryLockExclusive(handle.fd)
          break
        } catch (error) {
          if (error?.code !== 'EAGAIN' && error?.code !== 'EWOULDBLOCK') throw error
          if (Date.now() - started >= timeoutMs) {
            throw new LiaisonError('HOST_LOCK_TIMEOUT', 'Timed out waiting for the Host lock')
          }
          await sleep(25)
        }
      }
      const identity = {
        version: LOCK_VERSION,
        pid: process.pid,
        procStart: await processStartToken(process.pid),
        createdAt: new Date().toISOString(),
      }
      await handle.truncate(0)
      await handle.writeFile(`${JSON.stringify(identity)}\n`)
      let released = false
      return async () => {
        if (released) return
        released = true
        await handle.close().catch(() => {})
      }
    } catch (error) {
      await handle.close().catch(() => {})
      throw error
    }
  }
}

/**
 * The only lock protocol: the pinned upstream kernel flock. The lock file is
 * persistent; process death releases the kernel lock. A host without the
 * public native entry fails clearly instead of switching protocols.
 */
export async function createLock(path) {
  const flock = await loadFlock()
  if (flock === null) {
    throw new LiaisonError('LOCK_UNAVAILABLE',
      'Pinned kernel flock support (@deepseek-ai/node-addon-system/flock) is unavailable')
  }
  let handle
  try {
    handle = await open(path, 'a+', 0o600)
    await chmod(path, 0o600).catch(() => {})
    try {
      await flock.tryLockExclusive(handle.fd)
    } catch (error) {
      if (error?.code !== 'EAGAIN' && error?.code !== 'EWOULDBLOCK') {
        throw new LiaisonError('LOCK_UNAVAILABLE',
          `Kernel flock probe failed: ${String(error?.code ?? error?.message ?? 'unknown')}`)
      }
    }
    await handle.close()
    handle = undefined
    return new FlockLock(path, flock)
  } catch (error) {
    await handle?.close().catch(() => {})
    if (error instanceof LiaisonError) throw error
    throw new LiaisonError('LOCK_UNAVAILABLE',
      `Kernel flock is unavailable: ${String(error?.code ?? error?.message ?? 'unknown')}`)
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
    procRoot = '/proc',
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
    this.procRoot = procRoot
    this.recordPath = join(this.stateDir, 'host.json')
    this.lockPath = join(this.stateDir, 'host.lock')
    this.lockPromise = undefined
  }

  async #getLock() {
    if (this.lockPromise === undefined) this.lockPromise = createLock(this.lockPath)
    return this.lockPromise
  }

  async #inspect(record) {
    return inspectProcess(record, { procRoot: this.procRoot })
  }

  async #removeRecordByMarker(marker) {
    const file = await readJsonFile(this.recordPath)
    if (file === null) return true
    if (file.invalid === true) return false
    let markerOnDisk
    try { markerOnDisk = JSON.parse(file.raw)?.marker } catch { return false }
    if (markerOnDisk !== marker) return false
    return removeIfUnchanged(this.recordPath, file.raw)
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

  /** Read and verify the current ready managed Host without spawning or probing. */
  async readOwnedHost() {
    const loaded = await this.#loadRecord()
    if (loaded === null || loaded.invalid === true) return null
    if (loaded.record.state !== 'ready') return null
    const verification = await this.#inspect(loaded.record)
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
    const record = loaded.record
    const verification = record.pid === undefined
      ? { ok: false, reason: 'launching' }
      : await this.#inspect(record)
    const reachable = probe && verification.ok && record.state === 'ready'
      ? await probeOrigin(record.origin, timeoutMs)
      : false
    return {
      pid: record.pid ?? null,
      state: record.state,
      origin: record.origin ?? null,
      port: record.port ?? null,
      dshHome: record.dshHome,
      startedAt: record.startedAt,
      verified: verification.ok,
      reachable,
      reason: verification.reason,
    }
  }

  async #resolveRecordPid(record) {
    if (Number.isInteger(record.pid) && record.pid > 0) return { ok: true, record }
    const found = await findProcessesByMarker(record.marker, this.procRoot)
    if (!found.ok) return { ok: false, reason: 'unverified' }
    if (found.matches.length === 0) return { ok: true, record: null, missing: true }
    if (found.matches.length > 1) return { ok: false, reason: 'unverified', ambiguous: true }
    return { ok: true, record: { ...record, state: 'starting', ...found.matches[0] } }
  }

  async #terminateOutcome(record, verification, signal) {
    if (verification.reason === 'dead' || verification.reason === 'reused') {
      await this.#removeRecordByMarker(record.marker)
      return {
        stopped: true,
        reason: signal === 'SIGKILL' ? 'killed' : 'exited',
        pid: record.pid,
        signal,
        retained: false,
      }
    }
    return {
      stopped: false,
      reason: verification.ok ? 'still-running' : verification.reason,
      pid: record.pid,
      ...signal === undefined ? {} : { signal },
      retained: true,
    }
  }

  async #terminateRecord(record, { timeoutMs = 10_000, killTimeoutMs = 2_000 }) {
    const initial = await this.#inspect(record)
    if (!initial.ok) return await this.#terminateOutcome(record, initial, undefined)

    try { process.kill(record.pid, 'SIGTERM') } catch (error) {
      if (error?.code !== 'ESRCH') throw error
    }
    const deadline = Date.now() + timeoutMs
    let unverifiedSince
    while (Date.now() < deadline) {
      const current = await this.#inspect(record)
      if (!current.ok) {
        if (current.reason === 'dead' || current.reason === 'reused') {
          return await this.#terminateOutcome(record, current, 'SIGTERM')
        }
        if (current.reason === 'unverified') {
          if (await pidLiveness(record.pid) === 'dead') {
            return await this.#terminateOutcome(record, { ok: false, alive: false, reason: 'dead' }, 'SIGTERM')
          }
          unverifiedSince ??= Date.now()
          if (Date.now() - unverifiedSince >= 500) return await this.#terminateOutcome(record, current, 'SIGTERM')
          await sleep(50)
          continue
        }
        return await this.#terminateOutcome(record, current, 'SIGTERM')
      }
      unverifiedSince = undefined
      await sleep(100)
    }

    // Re-verify immediately before escalation so a recycled pid cannot be hit.
    const final = await this.#inspect(record)
    if (!final.ok) return await this.#terminateOutcome(record, final, 'SIGTERM')
    try { process.kill(record.pid, 'SIGKILL') } catch (error) {
      if (error?.code !== 'ESRCH') throw error
    }
    const killDeadline = Date.now() + killTimeoutMs
    while (Date.now() < killDeadline) {
      const current = await this.#inspect(record)
      if (!current.ok) {
        if (current.reason === 'dead' || current.reason === 'reused') {
          return await this.#terminateOutcome(record, current, 'SIGKILL')
        }
        if (current.reason === 'unverified' && await pidLiveness(record.pid) === 'dead') {
          return await this.#terminateOutcome(record, { ok: false, alive: false, reason: 'dead' }, 'SIGKILL')
        }
        // A dying process can briefly lose readable identity; give the kernel
        // signal time to complete before reporting the retained record.
        await sleep(50)
        continue
      }
      await sleep(50)
    }
    const last = await this.#inspect(record)
    return await this.#terminateOutcome(record, last, 'SIGKILL')
  }

  async #recoverStarting(record) {
    const resolved = await this.#resolveRecordPid(record)
    if (!resolved.ok) {
      throw new LiaisonError('HOST_RECORD_UNOWNED',
        'A write-ahead Host launch marker cannot be tied to a verified process')
    }
    if (resolved.record === null) {
      await this.#removeRecordByMarker(record.marker)
      return { recovered: false, reason: 'missing' }
    }
    const outcome = await this.#terminateRecord(resolved.record, { timeoutMs: 2_000, killTimeoutMs: 1_000 })
    if (outcome.retained === true) {
      throw new LiaisonError('HOST_RECORD_UNOWNED',
        `A write-ahead Host launch is still ${outcome.reason}; refusing to spawn a duplicate`)
    }
    return { recovered: true, reason: outcome.reason }
  }

  /** Attach to a verified live Host, reclaim a stale record, or spawn exactly one Host. */
  async ensureHost(options = {}) {
    await ensurePrivateDir(this.stateDir)
    const release = await (await this.#getLock()).acquire()
    try {
      const loaded = await this.#loadRecord()
      if (loaded !== null) {
        if (loaded.invalid === true) {
          await removeIfUnchanged(this.recordPath, loaded.raw)
        } else if (loaded.record.state !== 'ready') {
          await this.#recoverStarting(loaded.record)
        } else {
          const verification = await this.#inspect(loaded.record)
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

  async #assertProcessIdentitySupported() {
    if (!await procfsAvailable(this.procRoot)) {
      throw new LiaisonError('HOST_IDENTITY_UNSUPPORTED',
        'Managed Host spawn requires readable procfs process identity support')
    }
    const selfStart = await processStartToken(process.pid, this.procRoot)
    const selfEnvironment = await readProcessEnvironment(process.pid, this.procRoot)
    if (selfStart === undefined || selfEnvironment === undefined) {
      throw new LiaisonError('HOST_IDENTITY_UNSUPPORTED',
        'Managed Host spawn cannot verify process start time and environment identity')
    }
  }

  async #spawnHost(options) {
    await this.#assertProcessIdentitySupported()
    const dshHome = resolve(options.dshHome ?? this.defaultDshHome ?? this.env.DSH_HOME ?? join(homedir(), '.dsh'))
    const cwd = resolve(options.cwd ?? this.defaultCwd ?? process.cwd())
    const patches = (options.patches ?? this.defaultPatches).map(path => resolve(path))
    const port = Number(options.port ?? this.defaultPort)
    if (!Number.isInteger(port) || port < 0 || port > 65_535) {
      throw new LiaisonError('HOST_PORT_INVALID', 'Host port must be an integer from 0 to 65535')
    }

    const marker = randomUUID()
    const launchRecord = {
      version: RECORD_VERSION,
      state: 'launching',
      marker,
      dshHome,
      cwd,
      requestedPort: port,
      startedAt: new Date().toISOString(),
    }
    await writePrivateAtomic(this.recordPath, launchRecord)
    let ownershipRecord
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
      detached: true,
      stdio: ['ignore', stdout.handle.fd, stderr.handle.fd],
    })
    child.unref()
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
      if (child.pid === undefined) {
        throw new LiaisonError('HOST_SPAWN_FAILED', 'dsh web did not provide a pid')
      }
      const procStart = await processStartToken(child.pid, this.procRoot)
      if (procStart !== undefined) {
        ownershipRecord = {
          ...launchRecord,
          state: 'starting',
          pid: child.pid,
          procStart,
        }
        await writePrivateAtomic(this.recordPath, ownershipRecord)
      }
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
        ...(ownershipRecord ?? {
          ...launchRecord,
          state: 'starting',
          pid: child.pid,
          procStart: await processStartToken(child.pid, this.procRoot),
        }),
        state: 'ready',
        origin,
        port: Number(url.port),
        launchToken,
      }
      await writePrivateAtomic(this.recordPath, record)
      return hostHandle(record)
    } catch (error) {
      if (ownershipRecord !== undefined && child.exitCode === null) {
        await this.#terminateRecord(ownershipRecord, { timeoutMs: 2_000, killTimeoutMs: 1_000 }).catch(() => {})
      } else if (child.pid === undefined || child.exitCode !== null) {
        await this.#removeRecordByMarker(marker)
      }
      throw error
    } finally {
      await stdout.handle.close().catch(() => {})
      await stderr.handle.close().catch(() => {})
    }
  }

  /**
   * Stop only a Host whose process identity is still the exact one recorded.
   * A dead or reused pid is reclaimed; an unverified live process is reported
   * and its record is retained instead of being declared gone.
   */
  async stopHost({ timeoutMs = 10_000, killTimeoutMs = 2_000 } = {}) {
    await ensurePrivateDir(this.stateDir)
    const release = await (await this.#getLock()).acquire()
    try {
      const loaded = await this.#loadRecord()
      if (loaded === null || loaded.invalid === true) {
        if (loaded?.invalid === true) await removeIfUnchanged(this.recordPath, loaded.raw)
        return { stopped: false, reason: 'missing', pid: null, retained: false }
      }
      const resolved = await this.#resolveRecordPid(loaded.record)
      if (!resolved.ok) {
        return { stopped: false, reason: 'unverified', pid: loaded.record.pid ?? null, retained: true }
      }
      if (resolved.record === null) {
        await this.#removeRecordByMarker(loaded.record.marker)
        return { stopped: false, reason: 'missing', pid: null, retained: false }
      }
      return await this.#terminateRecord(resolved.record, { timeoutMs, killTimeoutMs })
    } finally {
      await release()
    }
  }
}
