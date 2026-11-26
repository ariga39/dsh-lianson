import { setTimeout as delay } from 'node:timers/promises'
import { Context } from '@deepseek-ai/cordis'
import { apply as gatewayApply, inject as gatewayInject, type ClientRemote } from '@deepseek-ai/dsh-api-gateway/client'
import type {} from '@deepseek-ai/dsh-api-remotes/types'
import sessionRemote from '@deepseek-ai/dsh-api-session-controller/remote'
import workspaceRemote from '@deepseek-ai/dsh-api-workspace-controller/remote'
import { installConnection, type ConnectionHandle, type ConnectionWebSocket } from '@deepseek-ai/dsh-client-connection/client'
import commandsRemote from '@deepseek-ai/dsh-commands/remote'
import permissionPresetsRemote from '@deepseek-ai/dsh-permission-presets/remote'
import type { SessionId } from '@deepseek-ai/dsh-session/types'
import TypertRegistry from '@deepseek-ai/dsh-typert-registry'
import userQuestionsRemote from '@deepseek-ai/dsh-user-questions/remote'
import type { AskUserQuestionAnswer } from '@deepseek-ai/dsh-user-questions/types'
import WebSocket from 'ws'

/** Authenticated external operator sharing one credential across HTTP and WebSocket. */
export interface Operator {
  /** Typed Remote surface mounted by this operator. */
  readonly remote: ClientRemote
  /** Bind the one Session this operator answers for. */
  bindSession(sessionId: SessionId): void
  /** Resolve the currently pending question with one external answer. */
  answerQuestion(answer: AskUserQuestionAnswer): void
  /** Release the Context and its WebSocket transport. */
  close(): Promise<void>
}

/** Exchange the Host launch token for one independent `interactions=answer` credential. */
export async function exchangeOperatorCredential(launchUrl: string): Promise<{ readonly cookie: string; readonly baseUrl: string }> {
  const application = new URL(launchUrl)
  const token = application.searchParams.get('token')
  if (token === null || token === '') throw new Error('launch URL carries no token')
  const connect = new URL('/connect', application.origin)
  connect.searchParams.set('token', token)
  connect.searchParams.set('interactions', 'answer')
  const response = await fetch(connect, { redirect: 'manual' })
  if (response.status !== 302) {
    throw new Error(`operator /connect returned HTTP ${String(response.status)}, expected 302`)
  }
  const setCookie = response.headers.get('set-cookie')
  if (setCookie === null) throw new Error('operator /connect omitted Set-Cookie')
  const cookie = setCookie.split(';', 1)[0]
  if (cookie === undefined || cookie === '') throw new Error('operator /connect returned an empty cookie')
  return { cookie, baseUrl: `${application.origin}/` }
}

/**
 * Authenticate and mount the public upstream Node client packages.
 * @param launchUrl - authenticated application URL printed by `dsh web`.
 * @returns the mounted operator and its typed Remote namespaces.
 */
export async function connectOperator(launchUrl: string): Promise<Operator> {
  const { cookie, baseUrl } = await exchangeOperatorCredential(launchUrl)
  const ctx = new Context()
  try {
    installConnection(ctx, {
      interactions: 'answer',
      transport: {
        streamBaseUrl: baseUrl,
        fetch: async (input, init) => {
          const headers = new Headers(init.headers)
          headers.set('cookie', cookie)
          return await fetch(new URL(input, baseUrl), { ...init, headers })
        },
        createWebSocket: (url) => {
          const socket: ConnectionWebSocket = new WebSocket(url, { headers: { cookie } })
          return socket
        },
      },
    })
    await ctx.plugin(TypertRegistry)
    await ctx.plugin({ apply: gatewayApply, inject: gatewayInject })
    for (const contribution of [
      sessionRemote,
      workspaceRemote,
      commandsRemote,
      permissionPresetsRemote,
      userQuestionsRemote,
    ]) {
      await ctx.remote.$mount(contribution)
    }
    const connection: ConnectionHandle = ctx.get('connection') as ConnectionHandle
    await waitFor(() => connection.generation.getSnapshot() !== undefined, 20_000, 'operator generation')

    const agentContext = ctx.extend()
    let boundSessionId: SessionId | undefined
    let pending: PromiseWithResolvers<AskUserQuestionAnswer> | undefined
    ctx.typert.contexts.registerClient('agent', {
      identity: () => undefined,
      resolve: id => id === boundSessionId ? agentContext : undefined,
    })
    agentContext.remote.$on('user-questions/request', async (request) => {
      const current = Promise.withResolvers<AskUserQuestionAnswer>()
      pending = current
      request.signal?.addEventListener('abort', () => {
        current.reject(request.signal?.reason instanceof Error ? request.signal.reason : new Error('question aborted'))
      }, { once: true })
      return await current.promise
    })

    let closed = false
    return {
      remote: ctx.remote,
      bindSession(sessionId: SessionId): void {
        boundSessionId = sessionId
      },
      answerQuestion(answer: AskUserQuestionAnswer): void {
        if (pending === undefined) throw new Error('no pending user question')
        pending.resolve(answer)
      },
      async close(): Promise<void> {
        if (closed) return
        closed = true
        pending?.reject(new Error('operator closed'))
        const disposing = ctx.fiber.dispose().catch(() => undefined)
        await Promise.race([disposing, delay(5_000)])
      },
    }
  } catch (error) {
    await ctx.fiber.dispose().catch(() => undefined)
    throw error
  }
}

async function waitFor(predicate: () => boolean, timeoutMs: number, label: string): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error(`timed out waiting for ${label}`)
    await delay(50)
  }
}
