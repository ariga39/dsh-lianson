import { resolve } from 'node:path'
import { createDeadline, type Deadline } from './deadline.ts'
import { ensurePrivateDirectory } from './fs-safe.ts'
import { acquireLock } from './flock.ts'
import { HostManager, type HostManagerOptions } from './host-manager.ts'
import { startIpcServer, type IpcServer } from './ipc-server.ts'
import { LiaisonError } from './errors.ts'
import { statePaths } from './paths.ts'

/**
 * Run the foreground daemon until an explicit `host.stop` or signal.
 * The daemon owns the IPC socket, host lifecycle lock, and one HostManager.
 * @param options - state directory, launch context, and timing overrides.
 * @returns process exit code.
 */
export async function runDaemon(options: HostManagerOptions): Promise<number> {
  await ensurePrivateDirectory(options.stateDir)
  const paths = statePaths(options.stateDir)
  const daemonLock = await acquireLock(paths.daemonLock, { waitMs: 0 })
  if (daemonLock === undefined) return 0
  const manager = new HostManager(options)
  const done = Promise.withResolvers<void>()
  const runtime: { server?: IpcServer } = {}
  const mutations = createMutationGate()
  let closing = false
  let shuttingDown: Promise<void> | undefined

  const shutdown = (): Promise<void> => {
    if (shuttingDown !== undefined) return shuttingDown
    closing = true
    shuttingDown = mutations.run(async () => {
      await runtime.server?.close()
      await manager.close()
      await daemonLock.release()
      done.resolve()
    })
    return shuttingDown
  }

  const scheduleShutdown = (): void => {
    const timer = setTimeout(() => { void shutdown() }, 50)
    timer.unref()
  }

  const handle = async (request: {
    readonly method: 'host.start' | 'host.status' | 'host.ping' | 'host.stop'
    readonly params: Record<string, unknown>
    readonly deadline: number
  }): Promise<unknown> => {
    if (closing && request.method !== 'host.stop') {
      throw new LiaisonError('DAEMON_STOPPING', 'the daemon is stopping')
    }
    if (request.method === 'host.start') {
      // Admission check plus a second check inside the mutation gate: an
      // expired queued start must never launch with stale options.
      const deadline = requestDeadline(request)
      assertRequestLive(deadline)
      const summary = await mutations.run(async () => {
        assertRequestLive(deadline)
        if (closing) throw new LiaisonError('DAEMON_STOPPING', 'the daemon is stopping')
        // The same manager/operator handles the start. Launch overrides apply
        // only at the launch decision inside HostManager; a live attach keeps
        // the existing Host's cwd, port, and patches.
        return await manager.start(launchOptionsOf(request.params, options), deadline)
      })
      return { ...summary, daemonPid: process.pid }
    }
    if (request.method === 'host.status') {
      const ensure = request.params.ensure === true
      if (ensure) {
        const deadline = requestDeadline(request)
        assertRequestLive(deadline)
        const summary = await mutations.run(async () => {
          assertRequestLive(deadline)
          if (closing) throw new LiaisonError('DAEMON_STOPPING', 'the daemon is stopping')
          return await manager.status(true, deadline)
        })
        return { ...summary, daemonPid: process.pid }
      }
      const inspection = await manager.status(false)
      return { ...inspection, daemonPid: process.pid }
    }
    if (request.method === 'host.ping') {
      const ping = await manager.ping()
      return { ...ping, daemonPid: process.pid }
    }
    const deadline = requestDeadline(request)
    assertRequestLive(deadline)
    let committed = false
    try {
      const keepHost = request.params.keepHost === true
      const outcome = await mutations.run(async () => {
        // Expiry before this point must not set the permanent closing fence
        // or withdraw the daemon; it is a pre-admission failure.
        assertRequestLive(deadline)
        return await manager.stop({ keepHost }, deadline, () => {
          // Single daemon-shutdown commit decision, delivered by HostManager
          // only after its own deadline and irreversible-action checks.
          committed = true
          closing = true
        })
      })
      return { ...outcome, daemonPid: process.pid }
    } finally {
      if (committed) {
        // Withdraw before the reply settles: an immediate next start must not
        // connect to a daemon that is already shutting down.
        await runtime.server?.withdraw().catch(() => undefined)
        scheduleShutdown()
      }
    }
  }

  runtime.server = await startIpcServer({
    socketPath: paths.socket,
    stateDir: options.stateDir,
    handle: async request => await handle(request),
  })
  const onSignal = (): void => { void shutdown() }
  process.once('SIGTERM', onSignal)
  process.once('SIGINT', onSignal)
  await done.promise
  process.off('SIGTERM', onSignal)
  process.off('SIGINT', onSignal)
  return 0
}

/** Convert the caller wall deadline into one monotonic local budget. */
function requestDeadline(request: { readonly deadline: number }): Deadline {
  return createDeadline(Math.max(0, request.deadline - Date.now()))
}

function assertRequestLive(deadline: Deadline): void {
  if (deadline.expired()) {
    throw new LiaisonError('IPC_TIMEOUT', 'the request deadline expired before it could run')
  }
}

/** Serialize manager selection/replacement and write transitions. */
function createMutationGate(): { run<T>(operation: () => Promise<T>): Promise<T> } {
  let tail: Promise<void> = Promise.resolve()
  return {
    run<T>(operation: () => Promise<T>): Promise<T> {
      const start = tail
      let release: () => void = () => {}
      tail = new Promise<void>((resolve) => { release = resolve })
      return start.then(operation).finally(release)
    },
  }
}

function launchOptionsOf(
  params: Readonly<Record<string, unknown>>,
  base: HostManagerOptions,
): Pick<HostManagerOptions, 'cwd' | 'port' | 'extraPatchPaths'> {
  const cwd = params.cwd
  if (cwd !== undefined && (typeof cwd !== 'string' || cwd === '')) {
    throw new LiaisonError('INVALID_ARGUMENT', 'host.start cwd must be a non-empty string')
  }
  const port = params.port
  if (port !== undefined && (typeof port !== 'number' || !Number.isSafeInteger(port) || port < 0 || port > 65_535)) {
    throw new LiaisonError('INVALID_ARGUMENT', 'host.start port must be an integer between 0 and 65535')
  }
  const patchPaths = params.patchPaths
  if (patchPaths !== undefined
    && (!Array.isArray(patchPaths) || !patchPaths.every(entry => typeof entry === 'string' && entry !== ''))) {
    throw new LiaisonError('INVALID_ARGUMENT', 'host.start patchPaths must be a string array')
  }
  const extra = patchPaths === undefined ? base.extraPatchPaths : patchPaths
  return {
    cwd: cwd === undefined ? base.cwd : resolve(cwd),
    port: port ?? base.port ?? 0,
    ...extra === undefined ? {} : { extraPatchPaths: extra },
  }
}
