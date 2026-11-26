import { spawn } from 'node:child_process'
import { existsSync } from 'node:fs'
import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { runDaemon } from './daemon.ts'
import { asLiaisonError, LiaisonError, wireErrorOf } from './errors.ts'
import { acquireLock } from './flock.ts'
import { removeIfExists } from './fs-safe.ts'
import { HOST_PING_DEADLINE_MS, HostManager, type HostManagerOptions, type StopOutcome } from './host-manager.ts'
import { callDaemon, isDaemonUnavailable } from './ipc-client.ts'
import type { IpcResultMap } from './ipc-protocol.ts'
import { resolveStateDir, statePaths } from './paths.ts'
import { redactSecrets, redactValue } from './redaction.ts'

const projectRoot = fileURLToPath(new URL('..', import.meta.url))
const binPath = fileURLToPath(new URL('../bin/dsh-liaison.ts', import.meta.url))
const DAEMON_BOOT_TIMEOUT_MS = 20_000
/** Bound on one active `host.start`, including its Host readiness deadline. */
export const START_CALL_TIMEOUT_MS = 150_000
/**
 * A stop may queue behind an active start, then wait for host.lock, SIGTERM
 * grace, and SIGKILL escalation. The reply deadline must cover that whole
 * sequence; a shorter client timeout would report failure and still let the
 * daemon stop the Host later.
 */
export const STOP_CALL_TIMEOUT_MS = START_CALL_TIMEOUT_MS + 60_000
/** Host ping uses one complete foreground budget; the CLI adds IPC transit. */
export const PING_CALL_TIMEOUT_MS = HOST_PING_DEADLINE_MS + 5_000

interface HostStartOptions {
  readonly port: number
  readonly patchPaths: readonly string[]
}

/**
 * Normal CLI entry.
 * @param argv - arguments after the executable.
 * @returns process exit code.
 */
export async function main(argv: readonly string[]): Promise<number> {
  try {
    const result = await dispatch(argv)
    const envelope = typeof result === 'object' && result !== null && !Array.isArray(result)
      ? { ok: true, ...result }
      : { ok: true, result }
    process.stdout.write(`${JSON.stringify(redactValue(envelope))}\n`)
    return 0
  } catch (error) {
    const failure = asLiaisonError(error)
    const wire = wireErrorOf(failure)
    process.stderr.write(`${JSON.stringify({
      ok: false,
      error: {
        code: wire.code,
        message: redactSecrets(wire.message),
        ...wire.details === undefined ? {} : { details: wire.details },
      },
    })}\n`)
    return 1
  }
}

async function dispatch(argv: readonly string[]): Promise<unknown> {
  const [group, command, ...rest] = argv
  if (group === '__daemon') return await runDaemonFromArgs(argv.slice(1))
  if (group !== 'host') {
    throw new LiaisonError('INVALID_ARGUMENT', 'usage: dsh-liaison host <start|status|ping|stop>')
  }
  if (command === 'start') return await startCommand(rest)
  if (command === 'status') return await statusCommand(rest)
  if (command === 'ping') return await pingCommand(rest)
  if (command === 'stop') return await stopCommand(rest)
  throw new LiaisonError('INVALID_ARGUMENT', 'usage: dsh-liaison host <start|status|ping|stop>')
}

async function startCommand(args: readonly string[]): Promise<IpcResultMap['host.start']> {
  const options = parseStartOptions(args)
  const stateDir = resolveStateDir()
  await ensureDaemon(stateDir, options)
  return await callDaemon(
    statePaths(stateDir).socket,
    'host.start',
    { cwd: process.cwd(), port: options.port, patchPaths: options.patchPaths },
    START_CALL_TIMEOUT_MS,
  )
}

async function statusCommand(args: readonly string[]): Promise<IpcResultMap['host.status']> {
  const ensure = args.includes('--ensure')
  const unknown = args.filter(arg => arg !== '--ensure')
  if (unknown.length > 0) throw new LiaisonError('INVALID_ARGUMENT', `unexpected status argument: ${unknown[0] ?? ''}`)
  const stateDir = resolveStateDir()
  if (ensure) {
    await ensureDaemon(stateDir, { port: 0, patchPaths: [] })
    return await callDaemon(statePaths(stateDir).socket, 'host.status', { ensure: true }, START_CALL_TIMEOUT_MS)
  }
  if (!existsSync(stateDir)) return { state: 'absent', daemonPid: null }
  try {
    return await callDaemon(statePaths(stateDir).socket, 'host.status', { ensure: false }, 5_000)
  } catch (error) {
    if (!isDaemonUnavailable(error)) throw error
  }
  const inspection = await localManager(stateDir, { port: 0, patchPaths: [] }).status(false)
  return { ...inspection, daemonPid: null }
}

async function pingCommand(args: readonly string[]): Promise<IpcResultMap['host.ping']> {
  if (args.length > 0) throw new LiaisonError('INVALID_ARGUMENT', `unexpected ping argument: ${args[0] ?? ''}`)
  const stateDir = resolveStateDir()
  await ensureDaemon(stateDir, { port: 0, patchPaths: [] })
  return await callDaemon(statePaths(stateDir).socket, 'host.ping', {}, PING_CALL_TIMEOUT_MS)
}

async function stopCommand(args: readonly string[]): Promise<IpcResultMap['host.stop']> {
  const known = new Set(['--keep-host'])
  const unknown = args.filter(arg => !known.has(arg))
  if (unknown.length > 0) throw new LiaisonError('INVALID_ARGUMENT', `unexpected stop argument: ${unknown[0] ?? ''}`)
  const keepHost = args.includes('--keep-host')
  const stateDir = resolveStateDir()
  if (!existsSync(stateDir)) {
    return { daemonPid: null, hostPid: null, stoppedHost: false, stopReason: 'absent' }
  }
  const paths = statePaths(stateDir)
  const deadline = Date.now() + STOP_CALL_TIMEOUT_MS
  while (Date.now() < deadline) {
    try {
      return await callDaemon(paths.socket, 'host.stop', { keepHost }, STOP_CALL_TIMEOUT_MS)
    } catch (error) {
      if (!isDaemonUnavailable(error)) throw error
    }
    const lock = await acquireLock(paths.daemonLock, { waitMs: 200 })
    if (lock === undefined) {
      await sleep(100)
      continue
    }
    try {
      const manager = localManager(stateDir, { port: 0, patchPaths: [] })
      let outcome: StopOutcome
      try {
        outcome = await manager.stop({ keepHost })
      } finally {
        await manager.close()
      }
      await removeIfExists(paths.socket)
      return { ...outcome, daemonPid: null }
    } finally {
      await lock.release()
    }
  }
  throw new LiaisonError('IPC_UNAVAILABLE', 'no daemon answered the stop request')
}

async function ensureDaemon(stateDir: string, options: HostStartOptions): Promise<void> {
  const paths = statePaths(stateDir)
  const args = [binPath, '__daemon', '--state-dir', stateDir, '--port', String(options.port)]
  for (const patch of options.patchPaths) args.push('--patch', patch)
  const deadline = Date.now() + DAEMON_BOOT_TIMEOUT_MS
  let lastSpawn = 0
  while (Date.now() < deadline) {
    try {
      await callDaemon(paths.socket, 'host.status', { ensure: false }, 2_000)
      return
    } catch (error) {
      if (!isDaemonUnavailable(error)) return
    }
    // A daemon that is withdrawing releases the socket just before it releases
    // the kernel lock; retrying the spawn makes an immediate next start wait
    // transparently for that shutdown instead of failing.
    if (Date.now() - lastSpawn > 500) {
      lastSpawn = Date.now()
      const child = spawn(process.execPath, args, {
        cwd: process.cwd(),
        env: process.env,
        detached: true,
        stdio: 'ignore',
      })
      child.unref()
    }
    await sleep(100)
  }
  throw new LiaisonError('IPC_UNAVAILABLE', 'the daemon did not start in time')
}

function localManager(stateDir: string, options: HostStartOptions): HostManager {
  const managerOptions: HostManagerOptions = {
    stateDir,
    projectRoot,
    cwd: process.cwd(),
    env: process.env,
    port: options.port,
    extraPatchPaths: options.patchPaths,
  }
  return new HostManager(managerOptions)
}

function parseStartOptions(args: readonly string[]): HostStartOptions {
  let port = 0
  const patchPaths: string[] = []
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]
    if (arg === '--port') {
      const value = args[index + 1]
      if (value === undefined || !/^\d+$/u.test(value)) {
        throw new LiaisonError('INVALID_ARGUMENT', '--port requires an integer')
      }
      port = Number(value)
      if (!Number.isSafeInteger(port) || port < 0 || port > 65_535) {
        throw new LiaisonError('INVALID_ARGUMENT', '--port must be between 0 and 65535')
      }
      index += 1
      continue
    }
    if (arg === '--patch') {
      const value = args[index + 1]
      if (value === undefined || value === '') {
        throw new LiaisonError('INVALID_ARGUMENT', '--patch requires a file path')
      }
      patchPaths.push(resolve(value))
      index += 1
      continue
    }
    throw new LiaisonError('INVALID_ARGUMENT', `unexpected start argument: ${arg ?? ''}`)
  }
  return { port, patchPaths }
}

async function runDaemonFromArgs(args: readonly string[]): Promise<number> {
  let stateDir: string | undefined
  let port = 0
  const patchPaths: string[] = []
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]
    if (arg === '--state-dir') {
      stateDir = args[index + 1]
      index += 1
      continue
    }
    if (arg === '--port') {
      const value = args[index + 1]
      if (value === undefined || !/^\d+$/u.test(value)) {
        throw new LiaisonError('INVALID_ARGUMENT', '--port requires an integer')
      }
      port = Number(value)
      index += 1
      continue
    }
    if (arg === '--patch') {
      const value = args[index + 1]
      if (value === undefined || value === '') {
        throw new LiaisonError('INVALID_ARGUMENT', '--patch requires a file path')
      }
      patchPaths.push(resolve(value))
      index += 1
      continue
    }
    throw new LiaisonError('INVALID_ARGUMENT', `unexpected daemon argument: ${arg ?? ''}`)
  }
  if (stateDir === undefined || stateDir === '') {
    throw new LiaisonError('INVALID_ARGUMENT', 'the daemon requires --state-dir')
  }
  return await runDaemon({
    stateDir,
    projectRoot,
    cwd: process.cwd(),
    env: process.env,
    port,
    extraPatchPaths: patchPaths,
  })
}

async function sleep(ms: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, ms)
  })
}
