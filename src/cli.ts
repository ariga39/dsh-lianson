/** Stateless `dsh-liaison` CLI speaking HTTP over the user-only Unix socket. */
import { request as httpRequest } from 'node:http'
import { Command, CommanderError } from 'commander'
import {
  ErrorResponseSchema,
  HealthResponseSchema,
  PingRequestSchema,
  PingResponseSchema,
  type ErrorResponse,
  type HealthResponse,
  type PingResponse,
} from './contract.ts'
import { resolveLiaisonSocketPath } from './socket-path.ts'

const REQUEST_TIMEOUT_MS = 10_000

interface OutputStream {
  write(chunk: string): unknown
}

/** Runtime dependencies injectable by focused tests. */
export interface CliEnvironment {
  readonly env?: NodeJS.ProcessEnv
  readonly stdout?: OutputStream
  readonly stderr?: OutputStream
}

interface RequestOptions {
  readonly socketPath: string
  readonly method: 'GET' | 'POST'
  readonly path: string
  readonly body?: unknown
  readonly timeoutMs?: number
}

class CliFailure extends Error {
  readonly code: string

  constructor(code: string, message: string) {
    super(message)
    this.code = code
  }
}

/**
 * Parse and execute one CLI invocation.
 * @param argv - arguments after the executable name.
 * @param environment - optional streams and environment for focused tests.
 * @returns the process exit code.
 */
export async function runCli(argv: readonly string[], environment: CliEnvironment = {}): Promise<number> {
  const env = environment.env ?? process.env
  const stdout = environment.stdout ?? process.stdout
  const stderr = environment.stderr ?? process.stderr
  let exitCode = 0

  const program = new Command()
    .name('dsh-liaison')
    .description('Inspect a running dsh-liaison plugin over its user-only Unix socket')
    .version('0.0.0')
    .exitOverride()
    .configureOutput({
      writeOut: text => { stdout.write(text) },
      writeErr: text => { stderr.write(text) },
    })

  program
    .command('status')
    .description('Print the plugin health document')
    .option('--socket <path>', 'override the liaison socket path')
    .action(async (options: { readonly socket?: string }) => {
      try {
        const health = await requestJson({
          socketPath: options.socket ?? resolveLiaisonSocketPath(env),
          method: 'GET',
          path: '/health',
        }, HealthResponseSchema)
        printJson(stdout, health)
      } catch (error) {
        printFailure(stdout, error)
        exitCode = 1
      }
    })

  program
    .command('ping')
    .description('Perform one authenticated loopback RPC and print the result')
    .option('--socket <path>', 'override the liaison socket path')
    .option('--nonce <text>', 'echo one caller-chosen nonce in the response')
    .action(async (options: { readonly socket?: string; readonly nonce?: string }) => {
      try {
        const input = PingRequestSchema.parse(options.nonce === undefined ? {} : { nonce: options.nonce })
        const pong = await requestJson({
          socketPath: options.socket ?? resolveLiaisonSocketPath(env),
          method: 'POST',
          path: '/ping',
          body: input,
        }, PingResponseSchema)
        printJson(stdout, pong)
      } catch (error) {
        printFailure(stdout, error)
        exitCode = 1
      }
    })

  try {
    await program.parseAsync([...argv], { from: 'user' })
  } catch (error) {
    if (error instanceof CommanderError) return error.exitCode
    printFailure(stdout, error)
    return 1
  }
  return exitCode
}

function printJson(stream: OutputStream, value: HealthResponse | PingResponse): void {
  stream.write(`${JSON.stringify(value)}\n`)
}

function printFailure(stream: OutputStream, error: unknown): void {
  const failure: ErrorResponse = {
    ok: false,
    error: {
      code: error instanceof CliFailure ? error.code : 'cli-failure',
      message: redactMessage(error instanceof Error ? error.message : String(error)),
    },
  }
  stream.write(`${JSON.stringify(ErrorResponseSchema.parse(failure))}\n`)
}

async function requestJson<T>(
  options: RequestOptions,
  schema: { parse(value: unknown): T },
): Promise<T> {
  const payload = options.body === undefined ? undefined : JSON.stringify(options.body)
  const response = await new Promise<{ readonly status: number; readonly body: string }>((resolve, reject) => {
    const request = httpRequest({
      socketPath: options.socketPath,
      path: options.path,
      method: options.method,
      headers: payload === undefined
        ? { accept: 'application/json' }
        : { accept: 'application/json', 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) },
      signal: AbortSignal.timeout(options.timeoutMs ?? REQUEST_TIMEOUT_MS),
    }, (incoming) => {
      const chunks: Buffer[] = []
      incoming.on('data', (chunk: Buffer) => { chunks.push(Buffer.from(chunk)) })
      incoming.once('end', () => {
        resolve({ status: incoming.statusCode ?? 0, body: Buffer.concat(chunks).toString('utf8') })
      })
      incoming.once('error', reject)
    })
    request.once('error', (error: NodeJS.ErrnoException) => {
      reject(new CliFailure(errorCode(error), redactMessage(error.message)))
    })
    request.end(payload)
  })

  let parsed: unknown
  try {
    parsed = JSON.parse(response.body)
  } catch {
    throw new CliFailure('endpoint-invalid-response', `HTTP ${String(response.status)} returned non-JSON output`)
  }
  if (response.status !== 200) {
    const remote = ErrorResponseSchema.safeParse(parsed)
    if (remote.success) throw new CliFailure(remote.data.error.code, remote.data.error.message)
    throw new CliFailure('endpoint-http-error', `HTTP ${String(response.status)}: ${redactMessage(response.body).slice(0, 500)}`)
  }
  try {
    return schema.parse(parsed)
  } catch {
    throw new CliFailure('endpoint-invalid-response', 'the endpoint returned an invalid JSON document')
  }
}

function errorCode(error: NodeJS.ErrnoException): string {
  if (error.name === 'AbortError' || error.name === 'TimeoutError' || error.code === 'ABORT_ERR') return 'endpoint-timeout'
  if (error.code === 'ENOENT' || error.code === 'ECONNREFUSED' || error.code === 'ENOTSOCK') return 'endpoint-unreachable'
  return 'endpoint-unreachable'
}

function redactMessage(message: string): string {
  return message
    .replace(/([?&\s])token=[^\s&)]+/giu, '$1token=<redacted>')
    .replace(/\b(cookie|set-cookie)\s*:\s*[^\n]+/giu, '$1: <redacted>')
}
