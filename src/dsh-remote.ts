/**
 * Node-side loopback Gateway client. The plugin authenticates itself to the
 * local `dsh` Web listener with the launch token it collects through
 * `webserver/entry-url`, keeps the cookie in memory only, and mounts the
 * generated Remote namespaces it needs.
 */

import { Context } from '@deepseek-ai/cordis'
import { brandString } from '@deepseek-ai/dsh-brand'
import { apply as gatewayApply, inject as gatewayInject } from '@deepseek-ai/dsh-api-gateway/client'
import sessionRemote from '@deepseek-ai/dsh-api-session-controller/remote'
import type { SessionRequestId, SessionWireEvent } from '@deepseek-ai/dsh-api-session-controller/types'
import type { WorkspaceId } from '@deepseek-ai/dsh-api-workspace-controller/types'
import type { SessionId } from '@deepseek-ai/dsh-session/types'
import workspaceRemote from '@deepseek-ai/dsh-api-workspace-controller/remote'
import { installConnection } from '@deepseek-ai/dsh-client-connection/client'
import type { ConnectionWebSocket } from '@deepseek-ai/dsh-client-connection/client'
import TypertRegistry from '@deepseek-ai/dsh-typert-registry'
import type { RemoteResult } from '@deepseek-ai/dsh-typert-protocol'
import WebSocket from 'ws'
import { LiaisonError } from './errors.ts'
import type { DshRemote, RemoteEventPage, RemoteModelCatalog, RemoteModelSelection } from './remote.ts'

/** One entry URL contributed by a Host authentication provider. */
export interface EntryUrl {
  readonly label: string
  readonly url: string
  readonly open: boolean
}

/** A loopback entry URL carrying exactly one non-empty launch token. */
export function selectAuthenticatedEntry(entries: readonly EntryUrl[]): string | undefined {
  for (const entry of entries) {
    let parsed: URL
    try {
      parsed = new URL(entry.url)
    } catch {
      continue
    }
    const tokens = parsed.searchParams.getAll('token')
    if (tokens.length === 1 && (tokens[0]?.length ?? 0) > 0) return parsed.href
  }
  return undefined
}

/**
 * Exchange a launch-token entry URL for an in-memory API cookie.
 * The redirect target is never followed and no credential is persisted.
 * @param entryUrl - authenticated entry URL from `webserver/entry-url`.
 * @param fetchImpl - fetch implementation; defaults to global fetch.
 * @returns the Host origin and the `name=value` cookie.
 */
export async function exchangeLoopbackCookie(
  entryUrl: string,
  fetchImpl: typeof fetch = fetch,
): Promise<{ origin: string; cookie: string }> {
  const entry = new URL(entryUrl)
  const token = entry.searchParams.get('token')
  if (token === null || token.length === 0) {
    throw new LiaisonError('dsh-unavailable', 'entry URL carries no launch token')
  }
  const connect = new URL('/connect', entry.origin)
  connect.searchParams.set('token', token)
  connect.searchParams.set('interactions', 'answer')
  let response: Response
  try {
    response = await fetchImpl(connect, { method: 'GET', redirect: 'manual', headers: { accept: 'text/plain' } })
  } catch (error) {
    throw new LiaisonError('dsh-unavailable', `loopback connect failed: ${error instanceof Error ? error.message : String(error)}`)
  }
  if (response.status !== 302) {
    throw new LiaisonError('dsh-unavailable', `loopback connect was refused with HTTP ${String(response.status)}`)
  }
  const setCookie = response.headers.get('set-cookie')
  const cookie = setCookie?.split(';', 1)[0]?.trim()
  if (cookie === undefined || cookie.length === 0) {
    throw new LiaisonError('dsh-unavailable', 'loopback connect returned no session cookie')
  }
  return { origin: entry.origin, cookie }
}

/**
 * Create an isolated Cordis Context carrying Typert, Connection, the Gateway
 * client, and the Workspace/Session Remote namespaces.
 * @param origin - Host origin without credentials.
 * @param cookie - in-memory API cookie.
 * @param signal - connect lifetime; abort disposes the context.
 * @param fetchImpl - fetch implementation for the HTTP carrier.
 * @param webSocketFactory - WebSocket factory for the event carrier.
 * @returns connected Host operations.
 */
export async function createLoopbackRemote(
  origin: string,
  cookie: string,
  signal: AbortSignal,
  fetchImpl: typeof fetch = fetch,
  webSocketFactory: (url: string) => ConnectionWebSocket = url => new WebSocket(url, { headers: { cookie } }),
): Promise<DshRemote> {
  const base = `${origin}/`
  const ctx = new Context()
  const abort = (): void => { void ctx.fiber.dispose() }
  signal.addEventListener('abort', abort, { once: true })
  try {
    await ctx.plugin(TypertRegistry)
    installConnection(ctx, {
      interactions: 'answer',
      transport: {
        streamBaseUrl: base,
        fetch: (input, init) => {
          const headers = new Headers(init.headers)
          headers.set('cookie', cookie)
          return fetchImpl(new URL(input, base), { ...init, headers })
        },
        createWebSocket: url => webSocketFactory(url),
      },
    })
    await ctx.plugin({ apply: gatewayApply, inject: gatewayInject })
    await ctx.remote.$mount(workspaceRemote)
    await ctx.remote.$mount(sessionRemote)
    await probe(ctx)
    if (signal.aborted) throw new LiaisonError('dsh-unavailable', 'loopback connection was disposed')
    return new GatewayRemote(ctx, origin)
  } catch (error) {
    signal.removeEventListener('abort', abort)
    await ctx.fiber.dispose()
    throw error instanceof LiaisonError
      ? error
      : new LiaisonError('dsh-unavailable', `loopback Gateway setup failed: ${error instanceof Error ? error.message : String(error)}`)
  }
}

async function probe(ctx: Context): Promise<void> {
  await unwrap(ctx.remote.session.modelCatalog())
}

class GatewayRemote implements DshRemote {
  readonly origin: string
  private readonly ctx: Context

  constructor(ctx: Context, origin: string) {
    this.ctx = ctx
    this.origin = origin
  }

  modelCatalog(): Promise<RemoteModelCatalog> {
    return unwrap(this.ctx.remote.session.modelCatalog())
  }

  async createWorkspace(path: string): Promise<string> {
    const value = await unwrap(this.ctx.remote.workspace.create({ path }))
    return value.workspace.workspaceId
  }

  async createSession(input: { workspaceId: string; sessionId: string }): Promise<string> {
    const value = await unwrap(this.ctx.remote.session.create({
      workspaceId: brandString<WorkspaceId>(input.workspaceId),
      sessionId: brandString<SessionId>(input.sessionId),
    }))
    return value.sessionId
  }

  async renameSession(sessionId: string, title: string): Promise<void> {
    await unwrap(this.ctx.remote.session.rename({ sessionId: brandString<SessionId>(sessionId), title }))
  }

  async selectModel(sessionId: string, selection: RemoteModelSelection): Promise<void> {
    await unwrap(this.ctx.remote.session.selectModel({
      sessionId: brandString<SessionId>(sessionId),
      provider: selection.provider,
      model: selection.model,
    }))
  }

  async beforeCursor(sessionId: string): Promise<number> {
    const page = await unwrap(this.ctx.remote.session.events({
      address: { kind: 'session', sessionId: brandString<SessionId>(sessionId) },
      maxMessages: 1,
    }))
    return page.cursor
  }

  async prompt(sessionId: string, requestId: string, text: string): Promise<void> {
    await unwrap(this.ctx.remote.session.prompt({
      requestId: brandString<SessionRequestId>(requestId),
      sessionId: brandString<SessionId>(sessionId),
      mode: 'queue',
      content: [{ type: 'text', text }],
    }))
  }

  async readEvents(sessionId: string): Promise<RemoteEventPage> {
    const address = { kind: 'session' as const, sessionId: brandString<SessionId>(sessionId) }
    const events: SessionWireEvent[] = []
    let cursor = 0
    let beforeSeq: number | undefined
    for (let pageIndex = 0; pageIndex < 12; pageIndex += 1) {
      const page = await unwrap(this.ctx.remote.session.events({
        address,
        maxMessages: 200,
        ...(beforeSeq === undefined ? {} : { beforeSeq }),
      }))
      cursor = Math.max(cursor, page.cursor)
      for (const record of page.records) events.push(record.event)
      const oldest = page.records.reduce<number | undefined>(
        (minimum, record) => minimum === undefined || record.event.seq < minimum ? record.event.seq : minimum,
        undefined,
      )
      if (!page.hasMore || oldest === undefined || oldest <= 0) break
      beforeSeq = oldest
    }
    return { events, cursor }
  }

  async dispose(): Promise<void> {
    await this.ctx.fiber.dispose()
  }
}

async function unwrap<T>(result: Promise<RemoteResult<T>>): Promise<T> {
  const settled = await result
  if (settled.ok) return settled.value
  const code = settled.error.code
  const kind = code.includes('not-found')
    ? 'not-found'
    : code === 'gateway/bad-request'
      ? 'usage'
      : code.includes('model-unavailable')
        ? 'model-unavailable'
        : 'internal'
  throw new LiaisonError(kind, settled.error.message, { remoteCode: code })
}
