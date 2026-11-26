import { spawn, type ChildProcess } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { homedir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { LiaisonError } from './errors.ts'
import { acquireLock } from './flock.ts'
import { ensurePrivateDirectory, isMissing, readOptionalFile, removeIfExists, writePrivateFile } from './fs-safe.ts'
import type { ObserverConfig } from './host-observer.ts'
import {
  OperatorConnection,
  type OperatorConnectionOptions,
  type OperatorTarget,
} from './operator-connection.ts'
import { statePaths, type StatePaths } from './paths.ts'
import {
  assertProcfsSupported,
  readProcStat,
  scanHostProcesses,
  verifyHostProcess,
  type HostIdentityExpectation,
} from './proc-identity.ts'
import type { RemoteResult } from '@deepseek-ai/dsh-typert-protocol'

/** One launching Host record, written before the child can publish readiness. */
export interface LaunchingHostRecord {
  readonly version: 1
  readonly state: 'launching'
  readonly launchId: string
  readonly observerPatchPath: string
  readonly readyFile: string
  readonly expectedCmdline: readonly string[]
  readonly startedAt: number
  readonly hostPid?: number
  readonly hostStartTime?: string
}

/** One committed Host record, including the launch credential. */
export interface ReadyHostRecord {
  readonly version: 1
  readonly state: 'ready'
  readonly launchId: string
  readonly observerPatchPath: string
  readonly hostPid: number
  readonly hostStartTime: string
  readonly expectedCmdline: readonly string[]
  readonly origin: string
  readonly port: number
  readonly launchToken: string
  readonly dshHome: string
  readonly cwd: string
  readonly startedAt: number
}

/** Discriminated ownership record persisted in `host.json`. */
export type HostRecord = LaunchingHostRecord | ReadyHostRecord

/** Public Host facts without credentials. */
export interface HostSummary {
  readonly state: 'ready'
  readonly launchId: string
  readonly hostPid: number
  readonly hostStartTime: string
  readonly origin: string
  readonly port: number
  readonly startedAt: number
  readonly attached: boolean
}

/** Read-only Host inspection. */
export type HostInspection =
  | { readonly state: 'absent' }
  | {
    readonly state: 'launching'
    readonly launchId: string
    readonly startedAt: number
    readonly identity: 'alive-verified' | 'dead-or-reused' | 'unverified' | 'ambiguous'
    readonly hostPid?: number
  }
  | {
    readonly state: 'ready'
    readonly launchId: string
    readonly hostPid: number
    readonly hostStartTime: string
    readonly origin: string
    readonly port: number
    readonly startedAt: number
    readonly identity: 'alive-verified' | 'dead-or-reused' | 'unverified'
    readonly reachable: boolean
  }

/** Truthful stop outcome. */
export interface StopOutcome {
  readonly hostPid: number | null
  readonly stoppedHost: boolean
  readonly stopReason: 'absent' | 'already-exited' | 'stopped' | 'kept'
}

/** Authenticated read-only ping evidence. */
export interface PingOutcome {
  readonly ok: true
  readonly hostPid: number
  readonly origin: string
  readonly port: number
  readonly sessions: number
  readonly latencyMs: number
}

/** Per-start launch overrides, ignored when an existing live Host is attached. */
export interface HostLaunchOverrides {
  readonly cwd?: string
  readonly port?: number
  readonly extraPatchPaths?: readonly string[]
}

interface EffectiveLaunchOptions {
  readonly cwd: string
  readonly port: number
  readonly extraPatchPaths: readonly string[]
}

/** Host manager construction inputs. */
export interface HostManagerOptions {
  readonly stateDir: string
  readonly projectRoot: string
  readonly cwd: string
  readonly env?: NodeJS.ProcessEnv
  readonly extraPatchPaths?: readonly string[]
  readonly port?: number
  readonly startTimeoutMs?: number
  readonly stopGraceMs?: number
  readonly killGraceMs?: number
  /** Foreground deadline for the initial operator attach, default 20s. */
  readonly operatorReadyTimeoutMs?: number
  readonly procRoot?: string
  readonly signalProcess?: (pid: number, signal: NodeJS.Signals) => void
  readonly spawnProcess?: typeof spawn
  readonly operator?: OperatorConnectionOptions
}

/** One normalized loopback origin. */
export interface NormalizedOrigin {
  readonly origin: string
  readonly port: number
  readonly protocol: 'http:' | 'https:'
}

const DEFAULT_START_TIMEOUT_MS = 120_000
const DEFAULT_STOP_GRACE_MS = 10_000
const DEFAULT_KILL_GRACE_MS = 2_000
const READY_POLL_MS = 25
const OBSERVER_MODULE = fileURLToPath(new URL('./host-observer.ts', import.meta.url))

/**
 * Own the single managed Host record and process lifecycle.
 * Start, attach, recovery, stop, and ping all read the same record.
 */
export class HostManager {
  private readonly paths: StatePaths
  private readonly options: HostManagerOptions
  private readonly procRoot: string
  private readonly signalProcess: (pid: number, signal: NodeJS.Signals) => void
  private readonly spawnProcess: typeof spawn
  private operator: OperatorConnection | undefined
  private attachmentClosed = false

  /**
   * @param options - state directory, launch context, timeouts, and test seams.
   */
  constructor(options: HostManagerOptions) {
    this.paths = statePaths(options.stateDir)
    this.options = options
    this.procRoot = options.procRoot ?? '/proc'
    this.signalProcess = options.signalProcess ?? ((pid, signal) => { process.kill(pid, signal) })
    this.spawnProcess = options.spawnProcess ?? spawn
  }

  /**
   * Launch or attach to the one managed Host.
   * @param overrides - launch cwd/port/patches used only if this call actually launches.
   */
  async start(overrides: HostLaunchOverrides = {}): Promise<HostSummary> {
    await assertProcfsSupported(this.procRoot)
    await ensurePrivateDirectory(this.paths.stateDir)
    const launch = this.effectiveLaunchOptions(overrides)
    const lock = await acquireLock(this.paths.hostLock, { waitMs: this.startTimeoutMs() })
    if (lock === undefined) throw new LiaisonError('HOST_START_TIMEOUT', 'host lifecycle lock was not acquired')
    try {
      const record = await this.readRecord()
      if (record === undefined) return await this.launchAndAttach(launch)
      if (record.state === 'launching') {
        await this.recoverLaunching(record)
        return await this.launchAndAttach(launch)
      }
      const identity = await verifyHostProcess(record.hostPid, record, this.procRoot)
      if (identity.kind === 'unverified') {
        throw new LiaisonError('HOST_IDENTITY_UNVERIFIED', 'the recorded Host process cannot be verified', {
          details: { reason: identity.reason, hostPid: record.hostPid },
        })
      }
      if (identity.kind === 'dead-or-reused') {
        await this.cleanupFiles()
        return await this.launchAndAttach(launch)
      }
      if (!await probeLoopback(record.origin)) {
        // A verified live Host whose endpoint is momentarily slow stays the
        // owner. Never signal it or tear down its sessions because a probe failed.
        throw new LiaisonError('HOST_UNREACHABLE', 'the verified Host is not reachable on its loopback origin', {
          details: { hostPid: record.hostPid, origin: record.origin },
        })
      }
      await this.attachOperator(record)
      return summaryOf(record)
    } finally {
      await lock.release()
    }
  }

  /**
   * Inspect the record without mutating ownership.
   * @param ensure - launch or attach when true.
   * @returns status facts.
   */
  async status(ensure = false): Promise<HostInspection | HostSummary> {
    if (ensure) return await this.start()
    const record = await this.readRecord()
    if (record === undefined) return { state: 'absent' }
    if (record.state === 'launching') {
      const identity = await this.resolveLaunchingIdentity(record)
      return {
        state: 'launching',
        launchId: record.launchId,
        startedAt: record.startedAt,
        identity: identity.kind,
        ...record.hostPid === undefined ? {} : { hostPid: record.hostPid },
      }
    }
    const identity = await verifyHostProcess(record.hostPid, record, this.procRoot)
    return {
      state: 'ready',
      launchId: record.launchId,
      hostPid: record.hostPid,
      hostStartTime: record.hostStartTime,
      origin: record.origin,
      port: record.port,
      startedAt: record.startedAt,
      identity: identity.kind,
      reachable: identity.kind === 'alive-verified' && await probeLoopback(record.origin),
    }
  }

  /** Read-only authenticated RPC used as the liveness proof. */
  async ping(): Promise<PingOutcome> {
    const record = await this.readRecord()
    if (record === undefined || record.state !== 'ready') {
      throw new LiaisonError('HOST_NOT_RUNNING', 'no managed Host is recorded')
    }
    const identity = await verifyHostProcess(record.hostPid, record, this.procRoot)
    if (identity.kind === 'unverified') {
      throw new LiaisonError('HOST_IDENTITY_UNVERIFIED', 'the recorded Host process cannot be verified', {
        details: { reason: identity.reason, hostPid: record.hostPid },
      })
    }
    if (identity.kind === 'dead-or-reused') {
      throw new LiaisonError('HOST_NOT_RUNNING', 'the recorded Host is not running')
    }
    if (!await probeLoopback(record.origin)) {
      throw new LiaisonError('HOST_UNREACHABLE', 'the recorded Host is not reachable on its loopback origin')
    }
    await this.attachOperator(record)
    const operator = this.operator
    if (operator === undefined) throw new LiaisonError('OPERATOR_UNAVAILABLE', 'no operator connection is available')
    const startedAt = Date.now()
    const result = await operator.withClient(
      remote => remote.session.list({}),
      15_000,
    )
    const value = unwrap(result)
    return {
      ok: true,
      hostPid: record.hostPid,
      origin: record.origin,
      port: record.port,
      sessions: value.items.length,
      latencyMs: Date.now() - startedAt,
    }
  }

  /**
   * Stop the Host, keeping ownership files only when explicitly requested.
   * @param options - `keepHost` stops the daemon's connection but never the Host.
   * @returns a truthful stop outcome.
   */
  async stop(options: { readonly keepHost?: boolean } = {}): Promise<StopOutcome> {
    // Fence attachment before waiting for the lifecycle lock: a ping that
    // already passed its read checks must not create a fresh reconnect loop
    // after keep-host shutdown begins.
    this.attachmentClosed = true
    const lock = await acquireLock(this.paths.hostLock, {
      waitMs: this.stopGraceMs() + this.killGraceMs() + 5_000,
    })
    if (lock === undefined) throw new LiaisonError('HOST_STOP_FAILED', 'host lifecycle lock was not acquired')
    try {
      const record = await this.readRecord()
      if (record === undefined) return { hostPid: null, stoppedHost: false, stopReason: 'absent' }
      if (record.state === 'launching') {
        const identity = await this.resolveLaunchingIdentity(record)
        if (identity.kind === 'unverified') {
          throw new LiaisonError('HOST_IDENTITY_UNVERIFIED', 'the launching Host cannot be verified', {
            details: { reason: identity.reason },
          })
        }
        if (identity.kind === 'ambiguous') {
          throw new LiaisonError('HOST_IDENTITY_AMBIGUOUS', 'more than one launching Host matched', {
            details: { candidates: identity.pids.length },
          })
        }
        if (identity.kind !== 'alive-verified') {
          await this.cleanupFiles()
          return { hostPid: null, stoppedHost: false, stopReason: 'already-exited' }
        }
        if (options.keepHost === true) {
          throw new LiaisonError('HOST_LAUNCH_INCOMPLETE', 'an interrupted launch cannot be kept')
        }
        await this.closeOperator()
        await this.terminate(identity.pid, identity.startTime, record)
        await this.cleanupFiles()
        return { hostPid: identity.pid, stoppedHost: true, stopReason: 'stopped' }
      }
      const identity = await verifyHostProcess(record.hostPid, record, this.procRoot)
      if (identity.kind === 'unverified') {
        throw new LiaisonError('HOST_IDENTITY_UNVERIFIED', 'the recorded Host cannot be verified', {
          details: { reason: identity.reason, hostPid: record.hostPid },
        })
      }
      if (identity.kind === 'dead-or-reused') {
        await this.cleanupFiles()
        return { hostPid: record.hostPid, stoppedHost: false, stopReason: 'already-exited' }
      }
      if (options.keepHost === true) {
        // Keeping never signals. Verify first so a dead/reused or unverifiable
        // record reports the truth instead of a false kept claim.
        await this.closeOperator()
        return { hostPid: record.hostPid, stoppedHost: false, stopReason: 'kept' }
      }
      await this.closeOperator()
      await this.terminateVerified(record)
      await this.cleanupFiles()
      return { hostPid: record.hostPid, stoppedHost: true, stopReason: 'stopped' }
    } finally {
      await lock.release()
    }
  }

  /** Close only this manager's operator connection and fence later attachment. */
  async close(): Promise<void> {
    this.attachmentClosed = true
    await this.closeOperator()
  }

  private async launchAndAttach(launch: EffectiveLaunchOptions): Promise<HostSummary> {
    const record = await this.launch(launch)
    // Readiness is committed: operator attachment is recoverable background
    // work, never physical launch rollback. A temporary /connect outage must
    // leave the verified Host and its ready record intact.
    await this.attachOperator(record)
    return summaryOf(record)
  }

  private async launch(launch: EffectiveLaunchOptions): Promise<ReadyHostRecord> {
    await ensurePrivateDirectory(this.paths.stateDir)
    await removeIfExists(this.paths.readyFile)
    const launchId = `launch-${randomUUID()}`
    const observerPatchPath = this.paths.observerPatch
    const readyFile = this.paths.readyFile
    const args = this.launchArguments(observerPatchPath, launch)
    const expectedCmdline = [process.execPath, ...args]
    const patch = [{
      insert: [{
        id: 'dsh-liaison-startup-observer',
        name: OBSERVER_MODULE,
        config: { readyFile, launchId } satisfies ObserverConfig,
      }],
    }]
    await writePrivateFile(observerPatchPath, JSON.stringify(patch, null, 2))
    const launching: LaunchingHostRecord = {
      version: 1,
      state: 'launching',
      launchId,
      observerPatchPath,
      readyFile,
      expectedCmdline,
      startedAt: Date.now(),
    }
    await writePrivateFile(this.paths.hostRecord, JSON.stringify(launching))
    let child: ChildProcess | undefined
    let activeRecord: LaunchingHostRecord | ReadyHostRecord = launching
    let spawnError: Error | undefined
    try {
      child = this.spawnProcess(process.execPath, args, {
        cwd: launch.cwd,
        env: this.hostEnvironment(launchId),
        detached: true,
        stdio: 'ignore',
      })
      child.once('error', (error) => { spawnError = error })
      const pid = child.pid
      if (pid === undefined) {
        await sleep(100)
        throw spawnError === undefined
          ? new LiaisonError('HOST_START_FAILED', 'the Host process did not spawn')
          : new LiaisonError('HOST_START_FAILED', 'the Host process did not spawn', { cause: spawnError })
      }
      const spawned: LaunchingHostRecord = { ...launching, hostPid: pid }
      activeRecord = spawned
      await writePrivateFile(this.paths.hostRecord, JSON.stringify(spawned))
      const startTime = await this.waitForStartTime(pid, () => spawnError)
      if (startTime === undefined) {
        throw new LiaisonError('HOST_IDENTITY_UNVERIFIED', 'the spawned Host did not expose procfs identity')
      }
      const launched: LaunchingHostRecord = { ...launching, hostPid: pid, hostStartTime: startTime }
      activeRecord = launched
      await writePrivateFile(this.paths.hostRecord, JSON.stringify(launched))
      child.unref()
      const ready = await this.waitForReady(child, readyFile, launchId, pid, () => spawnError)
      const origin = normalizeLoopbackOrigin(ready.origin)
      if (ready.port !== origin.port) {
        throw new LiaisonError('HOST_START_FAILED', 'the observer port does not match its origin')
      }
      if (!await probeLoopback(origin.origin)) {
        throw new LiaisonError('HOST_UNREACHABLE', 'the launched Host is not reachable on its loopback origin')
      }
      const record: ReadyHostRecord = {
        version: 1,
        state: 'ready',
        launchId,
        observerPatchPath,
        hostPid: pid,
        hostStartTime: startTime,
        expectedCmdline,
        origin: origin.origin,
        port: origin.port,
        launchToken: ready.launchToken,
        dshHome: this.dshHome(),
        cwd: launch.cwd,
        startedAt: launching.startedAt,
      }
      await writePrivateFile(this.paths.hostRecord, JSON.stringify(record))
      activeRecord = record
      await removeIfExists(readyFile)
      return record
    } catch (error) {
      // Never delete ownership evidence when the spawned process cannot be
      // verified gone. A later start/stop must resolve it, not lose it.
      await this.cleanupFailedLaunch(activeRecord)
      throw error
    }
  }

  /**
   * Resolve a launch failure truthfully.
   * @returns true when ownership was retained because the process is unverified or still alive.
   */
  private async cleanupFailedLaunch(record: LaunchingHostRecord | ReadyHostRecord): Promise<boolean> {
    if (record.hostPid === undefined) {
      await this.cleanupFiles().catch(() => undefined)
      return false
    }
    if (record.hostStartTime === undefined) {
      if (record.state !== 'launching') {
        await this.cleanupFiles().catch(() => undefined)
        return false
      }
      const identity = await this.resolveLaunchingIdentity(record)
      if (identity.kind === 'unverified' || identity.kind === 'ambiguous') return true
      if (identity.kind === 'dead-or-reused') {
        await this.cleanupFiles().catch(() => undefined)
        return false
      }
      try {
        await this.terminate(identity.pid, identity.startTime, record)
      } catch {
        return true
      }
      await this.cleanupFiles().catch(() => undefined)
      return false
    }
    const identity = await verifyHostProcess(record.hostPid, record, this.procRoot)
    if (identity.kind === 'unverified') return true
    if (identity.kind === 'dead-or-reused') {
      await this.cleanupFiles().catch(() => undefined)
      return false
    }
    try {
      await this.terminate(record.hostPid, record.hostStartTime, record)
    } catch {
      return true
    }
    await this.cleanupFiles().catch(() => undefined)
    return false
  }

  private async recoverLaunching(record: LaunchingHostRecord): Promise<void> {
    const identity = await this.resolveLaunchingIdentity(record)
    if (identity.kind === 'unverified') {
      throw new LiaisonError('HOST_IDENTITY_UNVERIFIED', 'the interrupted launch cannot be verified', {
        details: { reason: identity.reason },
      })
    }
    if (identity.kind === 'ambiguous') {
      throw new LiaisonError('HOST_IDENTITY_AMBIGUOUS', 'more than one interrupted Host matched', {
        details: { candidates: identity.pids.length },
      })
    }
    if (identity.kind === 'alive-verified') await this.terminate(identity.pid, identity.startTime, record)
    await this.cleanupFiles()
  }

  private async resolveLaunchingIdentity(record: LaunchingHostRecord): Promise<
    | { readonly kind: 'alive-verified'; readonly pid: number; readonly startTime: string }
    | { readonly kind: 'dead-or-reused' }
    | { readonly kind: 'ambiguous'; readonly pids: readonly number[] }
    | { readonly kind: 'unverified'; readonly reason: string }
  > {
    const expected: HostIdentityExpectation = {
      launchId: record.launchId,
      expectedCmdline: record.expectedCmdline,
      ...record.hostStartTime === undefined ? {} : { hostStartTime: record.hostStartTime },
    }
    if (record.hostPid !== undefined && record.hostStartTime !== undefined) {
      const result = await verifyHostProcess(record.hostPid, expected, this.procRoot)
      if (result.kind === 'alive-verified') return { kind: 'alive-verified', pid: record.hostPid, startTime: record.hostStartTime }
      if (result.kind === 'unverified') return result
    }
    const scan = await scanHostProcesses({
      launchId: record.launchId,
      expectedCmdline: record.expectedCmdline,
    }, this.procRoot)
    if (scan.kind === 'unique') {
      const stat = await readProcStat(scan.pid, this.procRoot)
      if (stat === undefined) return { kind: 'dead-or-reused' }
      return { kind: 'alive-verified', pid: scan.pid, startTime: stat.startTime }
    }
    if (scan.kind === 'none') return { kind: 'dead-or-reused' }
    return scan
  }

  private async waitForReady(
    child: ChildProcess,
    readyFile: string,
    launchId: string,
    pid: number,
    spawnFailure: () => Error | undefined,
  ): Promise<{ readonly origin: string; readonly port: number; readonly launchToken: string }> {
    return await new Promise((resolve, reject) => {
      let settled = false
      let reading = false
      const finish = (outcome: { readonly value: { origin: string; port: number; launchToken: string } } | { readonly error: Error }): void => {
        if (settled) return
        settled = true
        clearInterval(poll)
        clearTimeout(timeout)
        child.off('error', onError)
        child.off('exit', onExit)
        if ('value' in outcome) resolve(outcome.value)
        else reject(outcome.error)
      }
      const onError = (error: Error): void => { finish({ error }) }
      const onExit = (code: number | null, signal: NodeJS.Signals | null): void => {
        finish({
          error: new LiaisonError(
            'HOST_START_FAILED',
            `the Host exited before readiness (code ${String(code)}, signal ${String(signal)})`,
          ),
        })
      }
      const check = async (): Promise<void> => {
        if (settled || reading) return
        reading = true
        try {
          const failure = spawnFailure()
          if (failure !== undefined) {
            finish({ error: new LiaisonError('HOST_START_FAILED', 'the Host process failed to spawn', { cause: failure }) })
            return
          }
          const text = await readOptionalFile(readyFile)
          if (text === undefined || settled) return
          let parsed: unknown
          try {
            parsed = JSON.parse(text)
          } catch (error) {
            finish({ error: new LiaisonError('HOST_START_FAILED', 'the observer wrote an invalid ready record', { cause: error }) })
            return
          }
          const record = parseObserverReady(parsed)
          if (record.launchId !== launchId || record.pid !== pid) {
            finish({ error: new LiaisonError('HOST_START_FAILED', 'the ready record does not match this launch') })
            return
          }
          if (!record.ok) {
            finish({
              error: new LiaisonError('HOST_AUTH_UNAVAILABLE', 'the Host did not publish an authenticated entry URL', {
                details: { reason: record.reason ?? 'HOST_AUTH_UNAVAILABLE' },
              }),
            })
            return
          }
          finish({ value: { origin: record.origin, port: record.port, launchToken: record.launchToken } })
        } catch (error) {
          finish({
            error: new LiaisonError('HOST_START_FAILED', 'the observer ready record could not be read', { cause: error }),
          })
        } finally {
          reading = false
        }
      }
      const poll = setInterval(() => { void check() }, READY_POLL_MS)
      const timeout = setTimeout(() => {
        finish({ error: new LiaisonError('HOST_START_TIMEOUT', `the Host did not become ready within ${String(this.startTimeoutMs())}ms`) })
      }, this.startTimeoutMs())
      child.once('error', onError)
      child.once('exit', onExit)
      void check()
    })
  }

  private async waitForStartTime(pid: number, spawnFailure: () => Error | undefined): Promise<string | undefined> {
    const deadline = Date.now() + 5_000
    while (Date.now() < deadline) {
      const failure = spawnFailure()
      if (failure !== undefined) {
        throw new LiaisonError('HOST_START_FAILED', 'the Host process failed to spawn', { cause: failure })
      }
      const stat = await readProcStat(pid, this.procRoot).catch(() => undefined)
      if (stat !== undefined) return stat.startTime
      await sleep(10)
    }
    return undefined
  }

  private async terminateVerified(record: ReadyHostRecord): Promise<void> {
    await this.terminate(record.hostPid, record.hostStartTime, record)
  }

  private async terminate(
    pid: number,
    startTime: string,
    record: Pick<LaunchingHostRecord, 'launchId' | 'expectedCmdline'>,
  ): Promise<void> {
    const expectation: HostIdentityExpectation = {
      launchId: record.launchId,
      hostStartTime: startTime,
      expectedCmdline: record.expectedCmdline,
    }
    const verify = async (): Promise<'alive' | 'gone'> => {
      const identity = await verifyHostProcess(pid, expectation, this.procRoot)
      if (identity.kind === 'alive-verified') return 'alive'
      if (identity.kind === 'dead-or-reused') return 'gone'
      throw new LiaisonError('HOST_IDENTITY_UNVERIFIED', 'the Host identity cannot be verified', {
        details: { reason: identity.reason, hostPid: pid },
      })
    }
    const signal = (name: NodeJS.Signals): 'signalled' | 'gone' => {
      try {
        this.signalProcess(pid, name)
        return 'signalled'
      } catch (error) {
        if (isMissing(error)) return 'gone'
        throw new LiaisonError('HOST_STOP_FAILED', `the Host could not be signalled with ${name}`, {
          cause: error,
          details: { hostPid: pid },
        })
      }
    }

    if (await verify() === 'gone') return
    if (signal('SIGTERM') === 'gone') return
    if (await this.exitObserved(pid, this.stopGraceMs())) return
    // The PID may have exited and been reused during the grace period: prove
    // the exact process again before escalating to SIGKILL.
    if (await verify() === 'gone') return
    if (signal('SIGKILL') === 'gone') return
    if (await this.exitObserved(pid, this.killGraceMs())) return
    if (await verify() === 'gone') return
    throw new LiaisonError('HOST_STOP_FAILED', 'the Host did not exit after SIGTERM and SIGKILL', {
      details: { hostPid: pid },
    })
  }

  private async exitObserved(pid: number, timeoutMs: number): Promise<boolean> {
    try {
      return await this.waitForExit(pid, timeoutMs)
    } catch {
      return false
    }
  }

  private async waitForExit(pid: number, timeoutMs: number): Promise<boolean> {
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
      const stat = await readProcStat(pid, this.procRoot).catch((error: unknown) => {
        if (isMissing(error)) return undefined
        throw error
      })
      if (stat === undefined || stat.state === 'Z') return true
      await sleep(25)
    }
    return false
  }

  private launchArguments(observerPatchPath: string, launch: EffectiveLaunchOptions): string[] {
    const upstreamRoot = join(this.options.projectRoot, 'deepseek-harness')
    const args = [
      join(upstreamRoot, 'apps', 'cli', 'lib', 'bin.js'),
      '--profile', 'web',
      '--patch', observerPatchPath,
    ]
    for (const patch of launch.extraPatchPaths) args.push('--patch', patch)
    args.push('--no-open', '--host', '127.0.0.1', '--port', String(launch.port))
    return args
  }

  private effectiveLaunchOptions(overrides: HostLaunchOverrides): EffectiveLaunchOptions {
    return {
      cwd: overrides.cwd ?? this.options.cwd,
      port: overrides.port ?? this.options.port ?? 0,
      extraPatchPaths: overrides.extraPatchPaths ?? this.options.extraPatchPaths ?? [],
    }
  }

  private hostEnvironment(launchId: string): NodeJS.ProcessEnv {
    return {
      ...this.options.env ?? process.env,
      DSH_LIAISON_HOST_ID: launchId,
      DSH_TELEMETRY_DISABLED: '1',
      NODE_NO_WARNINGS: '1',
      SSH_CONNECTION: '',
      SSH_TTY: '',
    }
  }

  private dshHome(): string {
    const env = this.options.env ?? process.env
    const configured = env.DSH_HOME
    if (configured !== undefined && configured.trim() !== '') return configured
    return join(env.HOME !== undefined && env.HOME.trim() !== '' ? env.HOME : homedir(), '.dsh')
  }

  private async attachOperator(record: ReadyHostRecord): Promise<void> {
    if (this.attachmentClosed) {
      throw new LiaisonError('OPERATOR_UNAVAILABLE', 'operator attachment is closed for this lifecycle')
    }
    const target: OperatorTarget = { origin: record.origin, launchToken: record.launchToken }
    const timeoutMs = this.options.operatorReadyTimeoutMs ?? 20_000
    if (this.operator === undefined) this.operator = new OperatorConnection(this.options.operator)
    if (this.operator.matches(target)) await this.operator.waitReady(timeoutMs)
    else await this.operator.setHost(target, timeoutMs)
  }

  private async closeOperator(): Promise<void> {
    const operator = this.operator
    this.operator = undefined
    await operator?.close()
  }

  private async readRecord(): Promise<HostRecord | undefined> {
    let text: string | undefined
    try {
      text = await readOptionalFile(this.paths.hostRecord)
    } catch (error) {
      throw new LiaisonError('HOST_RECORD_INVALID', 'the Host ownership record cannot be read', { cause: error })
    }
    if (text === undefined) return undefined
    try {
      return parseHostRecord(JSON.parse(text))
    } catch (error) {
      throw new LiaisonError('HOST_RECORD_INVALID', 'the Host ownership record is malformed', { cause: error })
    }
  }

  private async cleanupFiles(): Promise<void> {
    await removeIfExists(this.paths.readyFile)
    await removeIfExists(this.paths.hostRecord)
    await removeIfExists(this.paths.observerPatch)
  }

  private startTimeoutMs(): number {
    return this.options.startTimeoutMs ?? DEFAULT_START_TIMEOUT_MS
  }

  private stopGraceMs(): number {
    return this.options.stopGraceMs ?? DEFAULT_STOP_GRACE_MS
  }

  private killGraceMs(): number {
    return this.options.killGraceMs ?? DEFAULT_KILL_GRACE_MS
  }
}

/**
 * Validate a loopback origin and preserve the effective HTTP/HTTPS port.
 * @param value - observer origin text.
 * @returns canonical origin and effective port.
 */
export function normalizeLoopbackOrigin(value: string): NormalizedOrigin {
  let url: URL
  try {
    url = new URL(value)
  } catch (error) {
    throw new LiaisonError('HOST_RECORD_INVALID', 'the Host origin is not a valid URL', { cause: error })
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new LiaisonError('HOST_RECORD_INVALID', 'the Host origin must use HTTP or HTTPS')
  }
  if (url.username !== '' || url.password !== '') {
    throw new LiaisonError('HOST_RECORD_INVALID', 'the Host origin must not carry credentials')
  }
  if (url.pathname !== '/' || url.hash !== '') {
    throw new LiaisonError('HOST_RECORD_INVALID', 'the Host origin must be a bare root URL')
  }
  const tokens = url.searchParams.getAll('token')
  if (url.search !== '' && (tokens.length !== 1 || tokens[0] === ''
    || [...url.searchParams.keys()].some(key => key !== 'token'))) {
    throw new LiaisonError('HOST_RECORD_INVALID', 'the Host origin carries only an optional token query')
  }
  const hostname = url.hostname.toLowerCase()
  if (hostname !== 'localhost' && hostname !== '127.0.0.1' && hostname !== '[::1]' && hostname !== '::1') {
    throw new LiaisonError('HOST_RECORD_INVALID', 'the Host origin must be loopback')
  }
  const port = url.port === '' ? (url.protocol === 'https:' ? 443 : 80) : Number(url.port)
  if (!Number.isSafeInteger(port) || port < 1 || port > 65_535) {
    throw new LiaisonError('HOST_RECORD_INVALID', 'the Host origin carries no effective port')
  }
  return { origin: url.origin, port, protocol: url.protocol }
}

/**
 * Probe the loopback root without credentials.
 * @param origin - bare loopback origin.
 * @returns true only for HTTP 200 or 401.
 */
async function probeLoopback(origin: string): Promise<boolean> {
  const controller = new AbortController()
  const timer = setTimeout(() => { controller.abort() }, 3_000)
  try {
    const response = await fetch(new URL('/', origin), { redirect: 'manual', signal: controller.signal })
    return response.status === 200 || response.status === 401
  } catch {
    return false
  } finally {
    clearTimeout(timer)
  }
}

function summaryOf(record: ReadyHostRecord): HostSummary {
  return {
    state: 'ready',
    launchId: record.launchId,
    hostPid: record.hostPid,
    hostStartTime: record.hostStartTime,
    origin: record.origin,
    port: record.port,
    startedAt: record.startedAt,
    attached: true,
  }
}

function parseHostRecord(value: unknown): HostRecord {
  const record = recordOf(value)
  if (record?.version !== 1 || (record.state !== 'launching' && record.state !== 'ready')) {
    throw new Error('unsupported host record')
  }
  const launchId = requiredString(record, 'launchId')
  const observerPatchPath = requiredString(record, 'observerPatchPath')
  const expectedCmdline = requiredStringArray(record, 'expectedCmdline')
  const startedAt = requiredInteger(record, 'startedAt')
  if (record.state === 'launching') {
    const readyFile = requiredString(record, 'readyFile')
    const hostPid = optionalInteger(record, 'hostPid')
    const hostStartTime = optionalString(record, 'hostStartTime')
    return {
      version: 1,
      state: 'launching',
      launchId,
      observerPatchPath,
      readyFile,
      expectedCmdline,
      startedAt,
      ...hostPid === undefined ? {} : { hostPid },
      ...hostStartTime === undefined ? {} : { hostStartTime },
    }
  }
  const hostPid = requiredInteger(record, 'hostPid')
  const hostStartTime = requiredString(record, 'hostStartTime')
  const origin = requiredString(record, 'origin')
  const port = requiredInteger(record, 'port')
  const launchToken = requiredString(record, 'launchToken')
  const dshHome = requiredString(record, 'dshHome')
  const cwd = requiredString(record, 'cwd')
  const normalized = normalizeLoopbackOrigin(origin)
  if (port !== normalized.port) throw new Error('record port does not match origin')
  return {
    version: 1,
    state: 'ready',
    launchId,
    observerPatchPath,
    hostPid,
    hostStartTime,
    expectedCmdline,
    origin: normalized.origin,
    port,
    launchToken,
    dshHome,
    cwd,
    startedAt,
  }
}

function parseObserverReady(value: unknown): {
  ok: boolean
  launchId: string
  pid: number
  origin: string
  port: number
  launchToken: string
  reason?: string
} {
  const record = recordOf(value)
  if (record === undefined) throw new LiaisonError('HOST_START_FAILED', 'the ready record is not an object')
  const ok = record.ok
  const launchId = record.launchId
  const pid = record.pid
  if (typeof ok !== 'boolean' || typeof launchId !== 'string' || typeof pid !== 'number') {
    throw new LiaisonError('HOST_START_FAILED', 'the ready record is malformed')
  }
  if (!ok) {
    const reason = typeof record.reason === 'string' ? record.reason : undefined
    return { ok: false, launchId, pid, origin: '', port: 0, launchToken: '', ...reason === undefined ? {} : { reason } }
  }
  if (typeof record.origin !== 'string' || typeof record.port !== 'number'
    || typeof record.launchToken !== 'string' || record.launchToken === '') {
    throw new LiaisonError('HOST_START_FAILED', 'the successful ready record is incomplete')
  }
  return { ok: true, launchId, pid, origin: record.origin, port: record.port, launchToken: record.launchToken }
}

function recordOf(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined
}

function requiredString(record: Record<string, unknown>, key: string): string {
  const value = record[key]
  if (typeof value !== 'string' || value === '') throw new Error(`${key} must be a non-empty string`)
  return value
}

function optionalString(record: Record<string, unknown>, key: string): string | undefined {
  const value = record[key]
  if (value === undefined) return undefined
  if (typeof value !== 'string' || value === '') throw new Error(`${key} must be a non-empty string`)
  return value
}

function requiredInteger(record: Record<string, unknown>, key: string): number {
  const value = record[key]
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${key} must be a non-negative integer`)
  }
  return value
}

function optionalInteger(record: Record<string, unknown>, key: string): number | undefined {
  const value = record[key]
  if (value === undefined) return undefined
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${key} must be a positive integer`)
  }
  return value
}

function requiredStringArray(record: Record<string, unknown>, key: string): string[] {
  const value = record[key]
  if (!Array.isArray(value) || value.length === 0 || !value.every(entry => typeof entry === 'string')) {
    throw new Error(`${key} must be a non-empty string array`)
  }
  return value
}

function unwrap<T>(result: RemoteResult<T>): T {
  if (!result.ok) throw new LiaisonError('OPERATOR_UNAVAILABLE', `${result.error.code}: ${result.error.message}`)
  return result.value
}

async function sleep(ms: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, ms)
  })
}
