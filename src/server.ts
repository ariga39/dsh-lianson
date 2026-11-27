/** In-process HTTP router served over the liaison Unix-domain socket. */

import type { IncomingMessage, ServerResponse } from 'node:http'
import { ZodError } from 'zod'
import { asLiaisonError, LiaisonError, redact } from './errors.ts'
import type { Ledger } from './ledger.ts'
import type { LoopbackStatus, RemoteSource } from './remote.ts'
import type { LiaisonPaths } from './paths.ts'
import {
  ROUTES, createRequestSchema, eventsRequestSchema, getRequestSchema, interactionAnswerRequestSchema,
  interactionApproveRequestSchema, interactionListRequestSchema, interactionRejectRequestSchema,
  listRequestSchema, promptRequestSchema, queueRequestSchema, reworkRequestSchema, statusRequestSchema,
  steerRequestSchema, stopRequestSchema, verifyModelRequestSchema, waitRequestSchema,
} from './protocol.ts'
import type { ServerEnvelope } from './protocol.ts'
import type { DelegationService } from './service.ts'
import { UdsServer } from './uds-server.ts'
import { PLUGIN_NAME, PLUGIN_VERSION } from './version.ts'

/** Maximum accepted request body. */
const MAX_BODY_BYTES = 4 * 1024 * 1024

/** Collaborators behind the HTTP surface. */
export interface LiaisonServerOptions {
  readonly paths: LiaisonPaths
  readonly ledger: Ledger
  readonly service: DelegationService
  readonly remote: RemoteSource
  readonly origin: () => string | undefined
}

/** HTTP-over-UDS server for the liaison commands. */
export class LiaisonServer {
  private readonly uds: UdsServer
  private started = false

  private readonly options: LiaisonServerOptions

  constructor(options: LiaisonServerOptions) {
    this.options = options
    this.uds = new UdsServer(options.paths.socketPath, options.paths.lockPath, (request, response) => this.handle(request, response))
  }

  /** Bind the socket or fail with `socket-in-use`. */
  async start(): Promise<void> {
    if (this.started) return
    await this.uds.start()
    this.started = true
  }

  /** Release the socket and lock. */
  async stop(): Promise<void> {
    if (!this.started) return
    this.started = false
    await this.uds.stop()
  }

  private async handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    try {
      if (request.method !== 'POST') {
        writeJson(response, 405, failure('usage', `method ${request.method ?? 'unknown'} is not supported`))
        return
      }
      const pathname = new URL(request.url ?? '/', 'http://dsh-liaison.invalid').pathname
      const body = await readJson(request)
      const envelope = await this.dispatch(pathname, body)
      writeJson(response, 200, envelope)
    } catch (error) {
      const failure = error instanceof ZodError
        ? new LiaisonError('usage', error.issues.map(issue => `${issue.path.join('.') || 'request'}: ${issue.message}`).join('; '))
        : asLiaisonError(error)
      writeJson(response, statusOf(failure.code), {
        ok: false,
        error: {
          code: failure.code,
          message: failure.message,
          ...(failure.details === undefined ? {} : { details: failure.details }),
        },
      })
    }
  }

  private async dispatch(pathname: string, body: unknown): Promise<ServerEnvelope> {
    switch (pathname) {
      case ROUTES.status: {
        statusRequestSchema.parse(body)
        return { ok: true, value: this.status() }
      }
      case ROUTES.create: {
        const input = createRequestSchema.parse(body)
        const result = await this.options.service.create(input)
        return { ok: true, value: result }
      }
      case ROUTES.get: {
        const input = getRequestSchema.parse(body)
        return { ok: true, value: { delegation: this.options.service.get(input.delegationId) } }
      }
      case ROUTES.list: {
        listRequestSchema.parse(body)
        return { ok: true, value: { delegations: this.options.service.list() } }
      }
      case ROUTES.prompt: {
        const input = promptRequestSchema.parse(body)
        return { ok: true, value: await this.options.service.prompt(input) }
      }
      case ROUTES.verifyModel: {
        const input = verifyModelRequestSchema.parse(body)
        return { ok: true, value: await this.options.service.verifyModel(input) }
      }
      case ROUTES.wait: {
        const input = waitRequestSchema.parse(body)
        return { ok: true, value: await this.options.service.wait(input) }
      }
      case ROUTES.events: {
        const input = eventsRequestSchema.parse(body)
        return { ok: true, value: await this.options.service.events(input) }
      }
      case ROUTES.steer: {
        const input = steerRequestSchema.parse(body)
        return { ok: true, value: await this.options.service.steer(input) }
      }
      case ROUTES.queue: {
        const input = queueRequestSchema.parse(body)
        return { ok: true, value: await this.options.service.queue(input) }
      }
      case ROUTES.stop: {
        const input = stopRequestSchema.parse(body)
        return { ok: true, value: await this.options.service.stop(input) }
      }
      case ROUTES.rework: {
        const input = reworkRequestSchema.parse(body)
        return { ok: true, value: await this.options.service.rework(input) }
      }
      case ROUTES.interactionList: {
        const input = interactionListRequestSchema.parse(body)
        return { ok: true, value: await this.options.service.interactionList(input) }
      }
      case ROUTES.interactionApprove: {
        const input = interactionApproveRequestSchema.parse(body)
        return { ok: true, value: this.options.service.interactionApprove(input.interactionId) }
      }
      case ROUTES.interactionReject: {
        const input = interactionRejectRequestSchema.parse(body)
        return { ok: true, value: this.options.service.interactionReject(input.interactionId) }
      }
      case ROUTES.interactionAnswer: {
        const input = interactionAnswerRequestSchema.parse(body)
        return {
          ok: true,
          value: await this.options.service.interactionAnswer({
            ...input,
            answers: input.answers.map(answer => ({
              id: answer.id,
              selected: answer.selected,
              ...(answer.custom === undefined ? {} : { custom: answer.custom }),
            })),
          }),
        }
      }
      default:
        throw new LiaisonError('usage', `unknown route ${pathname}`)
    }
  }

  private status(): Record<string, unknown> {
    const loopback: LoopbackStatus = this.options.remote.status()
    return {
      ok: true,
      plugin: { name: PLUGIN_NAME, version: PLUGIN_VERSION },
      dshHome: this.options.paths.dshHome,
      origin: this.options.origin() ?? loopback.origin ?? null,
      socketPath: this.options.paths.socketPath,
      ledgerPath: this.options.paths.ledgerPath,
      loopback,
      delegations: this.options.ledger.listDelegations().length,
    }
  }
}

function failure(code: string, message: string): ServerEnvelope {
  return { ok: false, error: { code, message } }
}

function statusOf(code: string): number {
  switch (code) {
    case 'usage':
    case 'invalid-cwd':
    case 'unsafe-cwd':
    case 'events/bad-range':
      return 400
    case 'not-found':
      return 404
    case 'conflict':
      return 409
    case 'dsh-unavailable':
    case 'socket-in-use':
      return 503
    default:
      return 500
  }
}

async function readJson(request: IncomingMessage): Promise<unknown> {
  const chunks: Uint8Array[] = []
  let total = 0
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
    total += buffer.byteLength
    if (total > MAX_BODY_BYTES) throw new LiaisonError('usage', `request body exceeds ${String(MAX_BODY_BYTES)} bytes`)
    chunks.push(buffer)
  }
  if (total === 0) return {}
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'))
  } catch {
    throw new LiaisonError('protocol', 'request body is not valid JSON')
  }
}

function writeJson(response: ServerResponse, status: number, envelope: ServerEnvelope): void {
  if (response.writableEnded) return
  const body = JSON.stringify(redact(envelope))
  response.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': String(Buffer.byteLength(body)),
    'cache-control': 'no-store',
  })
  response.end(body)
}
