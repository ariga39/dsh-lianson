/** Managed `dsh web` Host lifecycle: private state, attach-or-spawn, and verified shutdown. */

import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { chmod, mkdir, open, readdir, readFile, rename, stat, unlink } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { homedir } from 'node:os'
import { basename, dirname, isAbsolute, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { LiaisonError } from './errors.mjs'
import { redactSecrets } from './redaction.mjs'

export const REPO_ROOT = fileURLToPath(new URL('..', import.meta.url))
export const DSH_BIN = join(REPO_ROOT, 'deepseek-harness/apps/cli/lib/bin.js')
const DRAIN_BIN = fileURLToPath(new URL('./host-drain.mjs', import.meta.url))
const FLOCK_ANCHOR = join(REPO_ROOT, 'deepseek-harness/native/system/packages/entry/package.json')
export const HOST_MARKER_ENV = 'DSH_LIAISON_HOST_MARKER'
export const DRAIN_MARKER_ENV = 'DSH_LIAISON_DRAIN_MARKER'
export const DRAIN_HOST_MARKER_ENV = 'DSH_LIAISON_DRAIN_HOST_MARKER'
const RECORD_VERSION = 1
const LOCK_VERSION = 1

function isRecord(value) {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function sleep(ms) {
  return new Promise(resolvePromise => setTimeout(resolvePromise, ms))
}

/** Split stdout into complete newline-terminated lines and retain one bounded partial tail. */
export function readCompleteLines(buffer, chunk, max = 65_536) {
  const parts = `${buffer}${chunk}`.split('\n')
  const tail = parts.pop() ?? ''
  return {
    lines: parts.map(line => line.endsWith('\r') ? line.slice(0, -1) : line),
    buffer: tail.slice(-max),
  }
}

/** The drainer's child pid line, complete only. */
export function parseDrainPid(line) {
  const match = /^DSH_DRAIN_PID (\d+)$/u.exec(line)
  return match?.[1] === undefined ? undefined : Number(match[1])
}

/** The managed Host's complete readiness announcement line. */
export function parseLaunchAnnouncement(line) {
  return /^dsh web: (http:\/\/\S+)$/u.exec(line)?.[1]
}

/** State directory: explicit test override, XDG state home, then the user's state home. */
export function resolveStateDir(env = process.env) {
  if (typeof env.DSH_LIAISON_STATE_DIR === 'string' && env.DSH_LIAISON_STATE_DIR !== '') {
    return resolve(env.DSH_LIAISON_STATE_DIR)
  }
  // Durable state (including the delegation ledger) must survive an
  // XDG_RUNTIME_DIR clearing at logout or reboot, so runtime directories are
  // deliberately not consulted here.
  if (typeof env.XDG_STATE_HOME === 'string' && isAbsolute(env.XDG_STATE_HOME)) {
    return join(env.XDG_STATE_HOME, 'dsh-liaison')
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

export function hostStopSucceeded(outcome) {
  if (outcome?.stopped === true) return true
  return outcome?.reason === 'missing'
    || outcome?.reason === 'dead'
    || outcome?.reason === 'reused'
    || outcome?.reason === 'kept'
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

async function readProcessCommandLine(pid, procRoot = '/proc') {
  try {
    return (await readFile(join(procRoot, String(pid), 'cmdline'), 'utf8')).split('\0').filter(Boolean)
  } catch {
    return undefined
  }
}

/**
 * Find live processes carrying one launch marker. Only used for a write-ahead
 * record whose pid update was lost; multiple matches are deliberately ambiguous.
 * Command-line fragments let recovery distinguish the managed role from
 * descendants that inherited the same environment marker.
 */
export async function findProcessesByMarker(marker, {
  procRoot = '/proc',
  markerEnv = HOST_MARKER_ENV,
  cmdlineIncludes = [],
} = {}) {
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
    if (!environment.split('\0').includes(`${markerEnv}=${marker}`)) continue
    if (cmdlineIncludes.length > 0) {
      const commandLine = await readProcessCommandLine(pid, procRoot)
      if (commandLine === undefined || !cmdlineIncludes.every(value => commandLine.includes(value))) continue
    }
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
export async function inspectProcess(record, { procRoot = '/proc', markerEnv = HOST_MARKER_ENV } = {}) {
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
  const expected = `${markerEnv}=${record.marker}`
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
    drainerMarker: typeof value.drainerMarker === 'string' ? value.drainerMarker : undefined,
    drainerPid: Number.isInteger(value.drainerPid) ? value.drainerPid : undefined,
    drainerProcStart: typeof value.drainerProcStart === 'string' ? value.drainerProcStart : undefined,
  }
  if (state !== 'ready' && record.drainerMarker === undefined) {
    throw new LiaisonError('HOST_RECORD_INVALID', 'Launching Host record has no drainer marker')
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

  async #inspect(record, markerEnv = HOST_MARKER_ENV) {
    return inspectProcess(record, { procRoot: this.procRoot, markerEnv })
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
    const found = await findProcessesByMarker(record.marker, {
      procRoot: this.procRoot,
      markerEnv: HOST_MARKER_ENV,
      cmdlineIncludes: [this.binPath, 'web'],
    })
    if (!found.ok) return { ok: false, reason: 'unverified' }
    if (found.matches.length === 0) return { ok: true, record: null, missing: true }
    if (found.matches.length > 1) return { ok: false, reason: 'unverified', ambiguous: true }
    return { ok: true, record: { ...record, state: 'starting', ...found.matches[0] } }
  }

  async #resolveDrainer(record) {
    if (Number.isInteger(record.drainerPid) && record.drainerPid > 0) {
      return {
        ok: true,
        record: {
          pid: record.drainerPid,
          procStart: record.drainerProcStart,
          marker: record.drainerMarker,
        },
      }
    }
    if (typeof record.drainerMarker !== 'string' || record.drainerMarker === '') {
      return { ok: true, record: null, missing: true }
    }
    const found = await findProcessesByMarker(record.drainerMarker, {
      procRoot: this.procRoot,
      markerEnv: DRAIN_MARKER_ENV,
      cmdlineIncludes: [DRAIN_BIN],
    })
    if (!found.ok) return { ok: false, reason: 'unverified' }
    if (found.matches.length === 0) return { ok: true, record: null, missing: true }
    if (found.matches.length > 1) return { ok: false, reason: 'unverified', ambiguous: true }
    return {
      ok: true,
      record: { ...found.matches[0], marker: record.drainerMarker },
    }
  }

  async #terminateOutcome(record, verification, signal, { markerEnv = HOST_MARKER_ENV, removeRecord = true } = {}) {
    if (verification.reason === 'dead' || verification.reason === 'reused') {
      if (removeRecord) await this.#removeRecordByMarker(record.marker)
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
      markerEnv,
    }
  }

  async #terminateRecord(record, {
    timeoutMs = 10_000,
    killTimeoutMs = 2_000,
    markerEnv = HOST_MARKER_ENV,
    removeRecord = true,
  } = {}) {
    const options = { markerEnv, removeRecord }
    const initial = await this.#inspect(record, markerEnv)
    if (!initial.ok) return await this.#terminateOutcome(record, initial, undefined, options)

    try { process.kill(record.pid, 'SIGTERM') } catch (error) {
      if (error?.code !== 'ESRCH') throw error
    }
    const deadline = Date.now() + timeoutMs
    let unverifiedSince
    while (Date.now() < deadline) {
      const current = await this.#inspect(record, markerEnv)
      if (!current.ok) {
        if (current.reason === 'dead' || current.reason === 'reused') {
          return await this.#terminateOutcome(record, current, 'SIGTERM', options)
        }
        if (current.reason === 'unverified') {
          if (await pidLiveness(record.pid) === 'dead') {
            return await this.#terminateOutcome(record, { ok: false, alive: false, reason: 'dead' }, 'SIGTERM', options)
          }
          unverifiedSince ??= Date.now()
          if (Date.now() - unverifiedSince >= 500) {
            return await this.#terminateOutcome(record, current, 'SIGTERM', options)
          }
          await sleep(50)
          continue
        }
        return await this.#terminateOutcome(record, current, 'SIGTERM', options)
      }
      unverifiedSince = undefined
      await sleep(100)
    }

    // Re-verify immediately before escalation so a recycled pid cannot be hit.
    const final = await this.#inspect(record, markerEnv)
    if (!final.ok) return await this.#terminateOutcome(record, final, 'SIGTERM', options)
    try { process.kill(record.pid, 'SIGKILL') } catch (error) {
      if (error?.code !== 'ESRCH') throw error
    }
    const killDeadline = Date.now() + killTimeoutMs
    while (Date.now() < killDeadline) {
      const current = await this.#inspect(record, markerEnv)
      if (!current.ok) {
        if (current.reason === 'dead' || current.reason === 'reused') {
          return await this.#terminateOutcome(record, current, 'SIGKILL', options)
        }
        if (current.reason === 'unverified' && await pidLiveness(record.pid) === 'dead') {
          return await this.#terminateOutcome(record, { ok: false, alive: false, reason: 'dead' }, 'SIGKILL', options)
        }
        // A dying process can briefly lose readable identity; give the kernel
        // signal time to complete before reporting the retained record.
        await sleep(50)
        continue
      }
      await sleep(50)
    }
    const last = await this.#inspect(record, markerEnv)
    return await this.#terminateOutcome(record, last, 'SIGKILL', options)
  }

  async #recoverStarting(record) {
    const drainer = await this.#resolveDrainer(record)
    if (!drainer.ok) {
      throw new LiaisonError('HOST_RECORD_UNOWNED',
        'A write-ahead drainer marker cannot be tied to a verified process')
    }
    if (drainer.record !== null) {
      const stoppedDrainer = await this.#terminateRecord(drainer.record, {
        markerEnv: DRAIN_MARKER_ENV,
        removeRecord: false,
        timeoutMs: 1_000,
        killTimeoutMs: 500,
      })
      if (stoppedDrainer.retained === true) {
        throw new LiaisonError('HOST_RECORD_UNOWNED',
          `A write-ahead Host launcher is still ${stoppedDrainer.reason}; refusing to spawn a duplicate`)
      }
    }

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
        'Managed Host lifecycle requires Linux with readable procfs process identity')
    }
    const selfStart = await processStartToken(process.pid, this.procRoot)
    const selfEnvironment = await readProcessEnvironment(process.pid, this.procRoot)
    if (selfStart === undefined || selfEnvironment === undefined) {
      throw new LiaisonError('HOST_IDENTITY_UNSUPPORTED',
        'Managed Host lifecycle cannot verify Linux procfs process identity')
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
    const drainerMarker = randomUUID()
    const launchRecord = {
      version: RECORD_VERSION,
      state: 'launching',
      marker,
      drainerMarker,
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
      [DRAIN_MARKER_ENV]: drainerMarker,
      [DRAIN_HOST_MARKER_ENV]: marker,
    }
    delete env[HOST_MARKER_ENV]
    const args = [this.binPath, 'web']
    for (const patch of patches) args.push('--patch', patch)
    args.push('--no-open', '--host', '127.0.0.1', '--port', String(port))

    const drainEnv = {
      ...env,
      DSH_LIAISON_DRAIN_ARGS: JSON.stringify(args),
      DSH_LIAISON_DRAIN_CWD: cwd,
    }
    const drainer = spawn(process.execPath, [DRAIN_BIN], {
      cwd,
      env: drainEnv,
      detached: true,
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    let spawnError
    drainer.once('error', error => { spawnError = error })
    drainer.unref()
    if (drainer.pid !== undefined) {
      const drainerProcStart = await processStartToken(drainer.pid, this.procRoot)
      if (drainerProcStart !== undefined) {
        launchRecord.drainerPid = drainer.pid
        launchRecord.drainerProcStart = drainerProcStart
        await writePrivateAtomic(this.recordPath, launchRecord)
      }
    }

    let output = ''
    let errorOutput = ''
    let stdoutBuffer = ''
    let hostPid
    let launchUrl
    drainer.stdout.setEncoding('utf8')
    drainer.stderr.setEncoding('utf8')
    drainer.stdout.on('data', chunk => {
      output = `${output}${chunk}`.slice(-100_000)
      const read = readCompleteLines(stdoutBuffer, chunk)
      stdoutBuffer = read.buffer
      for (const line of read.lines) {
        if (hostPid === undefined) hostPid = parseDrainPid(line)
        if (launchUrl === undefined) launchUrl = parseLaunchAnnouncement(line)
      }
    })
    drainer.stderr.on('data', chunk => { errorOutput = `${errorOutput}${chunk}`.slice(-100_000) })
    const started = Date.now()
    try {
      while (Date.now() - started < this.spawnTimeoutMs) {
        if (spawnError !== undefined) {
          throw new LiaisonError('HOST_SPAWN_FAILED',
            `Could not start dsh web: ${redactSecrets(spawnError.message)}`)
        }
        if (hostPid !== undefined && ownershipRecord === undefined) {
          const procStart = await processStartToken(hostPid, this.procRoot)
          if (procStart !== undefined) {
            ownershipRecord = {
              ...launchRecord,
              state: 'starting',
              pid: hostPid,
              procStart,
            }
            await writePrivateAtomic(this.recordPath, ownershipRecord)
          }
        }
        if (launchUrl !== undefined && hostPid !== undefined) break
        if (drainer.exitCode !== null) {
          throw new LiaisonError('HOST_SPAWN_FAILED',
            `dsh web exited before readiness (${String(drainer.exitCode)}): ${redactSecrets(`${output}${errorOutput}`)}`)
        }
        await sleep(25)
      }
      if (launchUrl === undefined) {
        throw new LiaisonError('HOST_SPAWN_TIMEOUT',
          `dsh web did not become ready: ${redactSecrets(`${output}${errorOutput}`)}`)
      }
      if (hostPid === undefined) {
        throw new LiaisonError('HOST_SPAWN_FAILED', 'dsh web did not report a pid')
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
      const record = {
        ...(ownershipRecord ?? {
          ...launchRecord,
          state: 'starting',
          pid: hostPid,
          procStart: await processStartToken(hostPid, this.procRoot),
        }),
        state: 'ready',
        origin,
        port: Number(url.port),
        launchToken,
      }
      await writePrivateAtomic(this.recordPath, record)
      // Switch the detached drainer to discard-only. It keeps reading Host
      // output for its whole lifetime, so no unlinked file grows on disk.
      drainer.stdout.destroy()
      drainer.stderr.destroy()
      return hostHandle(record)
    } catch (error) {
      const cleanupRecord = ownershipRecord ?? (hostPid === undefined ? undefined : {
        ...launchRecord,
        state: 'starting',
        pid: hostPid,
        procStart: await processStartToken(hostPid, this.procRoot),
      })
      if (cleanupRecord !== undefined) {
        await this.#terminateRecord(cleanupRecord, { timeoutMs: 2_000, killTimeoutMs: 1_000 }).catch(() => {})
      } else if (drainer.pid === undefined || drainer.exitCode !== null) {
        await this.#removeRecordByMarker(marker)
      }
      throw error
    } finally {
      drainer.stdout.destroy()
      drainer.stderr.destroy()
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
      if (loaded.record.state !== 'ready') {
        const drainer = await this.#resolveDrainer(loaded.record)
        if (!drainer.ok) {
          return {
            stopped: false,
            reason: 'unverified',
            pid: loaded.record.drainerPid ?? loaded.record.pid ?? null,
            retained: true,
          }
        }
        if (drainer.record !== null) {
          const stoppedDrainer = await this.#terminateRecord(drainer.record, {
            markerEnv: DRAIN_MARKER_ENV,
            removeRecord: false,
            timeoutMs: 1_000,
            killTimeoutMs: 500,
          })
          if (stoppedDrainer.retained === true) {
            return {
              stopped: false,
              reason: stoppedDrainer.reason,
              pid: drainer.record.pid,
              retained: true,
            }
          }
        }
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
