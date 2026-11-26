import { access, readdir, readFile, stat } from 'node:fs/promises'
import { join } from 'node:path'
import { isPermissionDenied } from './fs-safe.ts'

/** The launch facts needed to prove one process is the managed root Host. */
export interface HostIdentityExpectation {
  readonly launchId: string
  readonly hostStartTime?: string | undefined
  readonly expectedCmdline: readonly string[]
}

/** Three-way identity result; unverified is never collapsed into dead. */
export type IdentityResult =
  | { readonly kind: 'alive-verified' }
  | { readonly kind: 'dead-or-reused' }
  | { readonly kind: 'unverified'; readonly reason: string }

/** Result of a pre-PID root-Host scan. */
export type ScanResult =
  | { readonly kind: 'unique'; readonly pid: number }
  | { readonly kind: 'none' }
  | { readonly kind: 'ambiguous'; readonly pids: readonly number[] }
  | { readonly kind: 'unverified'; readonly reason: string }

/** One parsed procfs process stat line. */
export interface ProcStat {
  readonly state: string
  readonly startTime: string
}

const HOST_MARKER_PREFIX = 'DSH_LIAISON_HOST_ID='

/**
 * Verify that Linux procfs exposes the three files root-Host identification needs.
 * @param procRoot - procfs root, default `/proc`.
 * @throws LiaisonError `HOST_IDENTITY_UNSUPPORTED` when any file is unavailable.
 */
export async function assertProcfsSupported(procRoot = '/proc'): Promise<void> {
  for (const name of ['self/stat', 'self/environ', 'self/cmdline']) {
    try {
      await access(join(procRoot, name))
    } catch (error) {
      const { LiaisonError } = await import('./errors.ts')
      throw new LiaisonError('HOST_IDENTITY_UNSUPPORTED', `procfs is not readable at ${join(procRoot, name)}`, {
        cause: error,
      })
    }
  }
}

/**
 * Read a process state and start time.
 * @param pid - process id.
 * @param procRoot - procfs root.
 * @returns parsed stat fields, or undefined when the process is gone.
 */
export async function readProcStat(pid: number, procRoot = '/proc'): Promise<ProcStat | undefined> {
  const text = await readProcFile(join(procRoot, String(pid), 'stat'))
  if (text === undefined) return undefined
  const close = text.lastIndexOf(')')
  if (close === -1) return undefined
  const fields = text.slice(close + 1).trim().split(/\s+/u)
  const state = fields[0]
  const startTime = fields[19]
  if (state === undefined || startTime === undefined) return undefined
  return { state, startTime }
}

/**
 * Read one process environment into exact `NAME=value` entries.
 * @param pid - process id.
 * @param procRoot - procfs root.
 * @returns entries, or undefined when the process is gone.
 */
export async function readProcEnviron(pid: number, procRoot = '/proc'): Promise<readonly string[] | undefined> {
  const text = await readProcFile(join(procRoot, String(pid), 'environ'))
  return text === undefined ? undefined : splitNul(text)
}

/**
 * Read one process argv.
 * @param pid - process id.
 * @param procRoot - procfs root.
 * @returns argv entries, or undefined when the process is gone.
 */
export async function readProcCmdline(pid: number, procRoot = '/proc'): Promise<readonly string[] | undefined> {
  const text = await readProcFile(join(procRoot, String(pid), 'cmdline'))
  return text === undefined ? undefined : splitNul(text)
}

/**
 * Prove a recorded process is still the same managed root Host.
 * @param pid - recorded process id.
 * @param expected - launch id, recorded start time, and exact launch argv.
 * @param procRoot - procfs root.
 * @returns the three-way identity result; never signals the process.
 */
export async function verifyHostProcess(
  pid: number,
  expected: HostIdentityExpectation,
  procRoot = '/proc',
): Promise<IdentityResult> {
  try {
    await access(join(procRoot, 'self', 'stat'))
  } catch (error) {
    return { kind: 'unverified', reason: `procfs is not readable: ${messageOf(error)}` }
  }
  const directory = join(procRoot, String(pid))
  try {
    await stat(directory)
  } catch (error) {
    if (isMissing(error)) return { kind: 'dead-or-reused' }
    return unverified('process directory is unreadable', error)
  }
  const flags = await readProcess(pid, procRoot, expected.expectedCmdline)
  if (flags.kind === 'gone' || flags.kind === 'mismatch') return { kind: 'dead-or-reused' }
  if (flags.kind === 'unverified') return flags
  const { stat: processStat, environ } = flags
  if (processStat.state === 'Z') return { kind: 'dead-or-reused' }
  if (expected.hostStartTime !== undefined && processStat.startTime !== expected.hostStartTime) {
    return { kind: 'dead-or-reused' }
  }
  if (!environ.includes(`${HOST_MARKER_PREFIX}${expected.launchId}`)) return { kind: 'dead-or-reused' }
  return { kind: 'alive-verified' }
}

/**
 * Scan same-UID non-zombie processes for the unique managed root Host.
 * A process must carry the launch marker and match the exact launch argv.
 * @param expected - launch id and exact launch argv.
 * @param procRoot - procfs root.
 * @param uid - owner to inspect, default the current effective uid.
 * @returns unique, none, ambiguous, or unverified.
 */
export async function scanHostProcesses(
  expected: Omit<HostIdentityExpectation, 'hostStartTime'>,
  procRoot = '/proc',
  uid = process.getuid?.() ?? -1,
): Promise<ScanResult> {
  let entries: string[]
  try {
    entries = await readdir(procRoot)
  } catch (error) {
    return { kind: 'unverified', reason: `procfs is not enumerable: ${messageOf(error)}` }
  }
  const candidates: number[] = []
  for (const entry of entries) {
    if (!/^\d+$/u.test(entry)) continue
    const pid = Number(entry)
    if (pid === process.pid) continue
    let ownerUid: number
    try {
      ownerUid = (await stat(join(procRoot, entry))).uid
    } catch (error) {
      if (isMissing(error)) continue
      return { kind: 'unverified', reason: `process directory is unreadable: ${messageOf(error)}` }
    }
    if (ownerUid !== uid) continue
    const flags = await readProcess(pid, procRoot, expected.expectedCmdline)
    if (flags.kind === 'gone' || flags.kind === 'mismatch') continue
    if (flags.kind === 'unverified') return flags
    if (flags.stat.state === 'Z') continue
    if (!flags.environ.includes(`${HOST_MARKER_PREFIX}${expected.launchId}`)) continue
    candidates.push(pid)
  }
  if (candidates.length === 0) return { kind: 'none' }
  if (candidates.length > 1) return { kind: 'ambiguous', pids: candidates }
  const [only] = candidates
  if (only === undefined) return { kind: 'none' }
  return { kind: 'unique', pid: only }
}

type ProcessRead =
  | {
    readonly kind: 'ok'
    readonly stat: ProcStat
    readonly environ: readonly string[]
  }
  | { readonly kind: 'gone' }
  | { readonly kind: 'mismatch' }
  | { readonly kind: 'unverified'; readonly reason: string }

async function readProcess(
  pid: number,
  procRoot: string,
  expectedCmdline?: readonly string[],
): Promise<ProcessRead> {
  const directory = join(procRoot, String(pid))
  let statText: string
  let cmdlineText: string
  let environText: string
  try {
    statText = await readFile(join(directory, 'stat'), 'utf8')
    cmdlineText = await readFile(join(directory, 'cmdline'), 'utf8')
    if (expectedCmdline !== undefined && !sameArgv(splitNul(cmdlineText), expectedCmdline)) {
      return { kind: 'mismatch' }
    }
    environText = await readFile(join(directory, 'environ'), 'utf8')
  } catch (error) {
    if (isMissing(error)) return { kind: 'gone' }
    if (isPermissionDenied(error)) return { kind: 'unverified', reason: `process files are unreadable: ${messageOf(error)}` }
    return { kind: 'unverified', reason: `process files could not be read: ${messageOf(error)}` }
  }
  const close = statText.lastIndexOf(')')
  if (close === -1) return { kind: 'unverified', reason: `malformed stat for pid ${String(pid)}` }
  const fields = statText.slice(close + 1).trim().split(/\s+/u)
  const state = fields[0]
  const startTime = fields[19]
  if (state === undefined || startTime === undefined) {
    return { kind: 'unverified', reason: `malformed stat for pid ${String(pid)}` }
  }
  return {
    kind: 'ok',
    stat: { state, startTime },
    environ: splitNul(environText),
  }
}

async function readProcFile(path: string): Promise<string | undefined> {
  try {
    return await readFile(path, 'utf8')
  } catch (error) {
    if (isMissing(error)) return undefined
    throw error
  }
}

function unverified(reason: string, error: unknown): IdentityResult {
  return { kind: 'unverified', reason: `${reason}: ${messageOf(error)}` }
}

function splitNul(text: string): string[] {
  return text.split('\0').filter((entry, index, all) => !(entry === '' && index === all.length - 1))
}

function sameArgv(actual: readonly string[], expected: readonly string[]): boolean {
  if (actual.length !== expected.length) return false
  return actual.every((value, index) => value === expected[index])
}

function isMissing(error: unknown): boolean {
  const code = (error as NodeJS.ErrnoException | null)?.code
  return code === 'ENOENT' || code === 'ESRCH'
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
