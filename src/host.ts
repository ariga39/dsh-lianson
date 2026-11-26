import { spawn, type ChildProcess } from 'node:child_process'
import { once } from 'node:events'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { createLaunchLineCollector, findLaunchUrl, redact } from './launch-lines.ts'

/** Options for one real `dsh web --no-open` launch. */
export interface StartDshWebOptions {
  readonly projectRoot: string
  readonly workspace: string
  readonly dshHome: string
  readonly agentsHome: string
  readonly bundledSkillDir: string
  readonly patchPath: string
  readonly timeoutMs?: number
}

/** Running Host process and its retained diagnostics. */
export interface RunningDshWeb {
  readonly launchUrl: string
  readonly coldStartMs: number
  readonly pid: number | undefined
  /** Complete stderr received through close, redacted only at presentation time. */
  stderr(): string
  /** Complete stdout received through close. */
  stdout(): string
  /** Ask the Host to stop, escalating once, and wait for its exit. */
  stop(): Promise<void>
}

const DEFAULT_READY_TIMEOUT_MS = 90_000
const STOP_GRACE_MS = 10_000

/** Start the pinned upstream Host from its normal built CLI entry point. */
export async function startDshWeb(options: StartDshWebOptions): Promise<RunningDshWeb> {
  const upstreamRoot = join(options.projectRoot, 'deepseek-harness')
  const bin = join(upstreamRoot, 'apps/cli/lib/bin.js')
  const startedAt = Date.now()
  const child = spawn(process.execPath, [
    bin,
    '--profile', 'web',
    '--patch', options.patchPath,
    '--no-open',
    '--host', '127.0.0.1',
    '--port', '0',
  ], {
    cwd: upstreamRoot,
    env: cleanEnvironment(options),
    stdio: ['ignore', 'pipe', 'pipe'],
  })

  const stdoutLines: string[] = []
  const stdoutChunks: string[] = []
  const stderrChunks: string[] = []
  const stdoutRemainder = createLaunchLineCollector()
  const stderrRemainder = createLaunchLineCollector()
  const acceptStdout = (chunk: string): void => {
    stdoutChunks.push(chunk)
    stdoutLines.push(...stdoutRemainder.push(chunk))
  }
  const acceptStderr = (chunk: string): void => {
    stderrChunks.push(chunk)
    stderrRemainder.push(chunk)
  }
  child.stdout?.setEncoding('utf8')
  child.stderr?.setEncoding('utf8')
  child.stdout?.on('data', acceptStdout)
  child.stderr?.on('data', acceptStderr)

  let launchUrl: string
  let coldStartMs: number
  try {
    launchUrl = await waitForLaunchUrl(child, stdoutLines, stdoutChunks, stderrChunks, options.timeoutMs ?? DEFAULT_READY_TIMEOUT_MS)
    coldStartMs = Date.now() - startedAt
  } catch (error) {
    await stopChild(child)
    const diagnostics = [
      `stdout:\n${stdoutChunks.join('')}`,
      `stderr:\n${stderrChunks.join('')}`,
    ].join('\n')
    throw new Error(`${error instanceof Error ? error.message : String(error)}\n${redact(diagnostics)}`, { cause: error })
  }

  return {
    launchUrl,
    coldStartMs,
    get pid() { return child.pid },
    stderr(): string {
      return stderrChunks.join('')
    },
    stdout(): string {
      return stdoutChunks.join('')
    },
    async stop(): Promise<void> {
      // Flush any unterminated tail before the streams close permanently.
      stdoutLines.push(...stdoutRemainder.finish())
      stderrRemainder.finish()
      await stopChild(child)
    },
  }
}

function cleanEnvironment(options: StartDshWebOptions): NodeJS.ProcessEnv {
  const inherited = Object.fromEntries(Object.entries(process.env).filter(([name]) =>
    !/(?:KEY|SECRET|TOKEN|PASSWORD)/iu.test(name)))
  return {
    ...inherited,
    DSH_HOME: options.dshHome,
    DSH_AGENTS_HOME: options.agentsHome,
    DSH_BUNDLED_SKILL_DIR: options.bundledSkillDir,
    DSH_TELEMETRY_DISABLED: '1',
    NODE_NO_WARNINGS: '1',
    SSH_CONNECTION: '',
    SSH_TTY: '',
  }
}

async function waitForLaunchUrl(
  child: ChildProcess,
  lines: readonly string[],
  stdoutChunks: readonly string[],
  stderrChunks: readonly string[],
  timeoutMs: number,
): Promise<string> {
  return await new Promise<string>((resolve, reject) => {
    let settled = false
    const finish = (outcome: { readonly url: string } | { readonly error: Error }): void => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      child.stdout?.off('data', onData)
      child.stderr?.off('data', onData)
      child.off('error', onError)
      child.off('exit', onExit)
      if ('url' in outcome) resolve(outcome.url)
      else reject(outcome.error)
    }
    const onData = (): void => {
      const url = findLaunchUrl(lines)
      if (url !== undefined) finish({ url })
    }
    const onError = (error: Error): void => {
      finish({ error })
    }
    const onExit = (code: number | null, signal: NodeJS.Signals | null): void => {
      finish({
        error: new Error(
          `dsh web exited before readiness (code ${String(code)}, signal ${String(signal)}):\n`
          + redact(`${stdoutChunks.join('')}\n${stderrChunks.join('')}`),
        ),
      })
    }
    const timer = setTimeout(() => {
      finish({
        error: new Error(
          `dsh web did not print an authenticated launch line within ${String(timeoutMs)}ms:\n`
          + redact(`${stdoutChunks.join('')}\n${stderrChunks.join('')}`),
        ),
      })
    }, timeoutMs)
    child.stdout?.on('data', onData)
    child.stderr?.on('data', onData)
    child.once('error', onError)
    child.once('exit', onExit)
    const existing = findLaunchUrl(lines)
    if (existing !== undefined) finish({ url: existing })
  })
}

async function stopChild(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return
  const exited = once(child, 'exit').then(() => undefined)
  child.kill('SIGTERM')
  const forced = setTimeout(() => {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL')
  }, STOP_GRACE_MS)
  forced.unref()
  try {
    await Promise.race([exited, delay(STOP_GRACE_MS + 2_000)])
  } finally {
    clearTimeout(forced)
  }
}
