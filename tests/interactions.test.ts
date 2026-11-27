import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { Context } from '@deepseek-ai/cordis'
import { brandString } from '@deepseek-ai/dsh-brand'
import type { ToolCallId } from '@deepseek-ai/dsh-llm/types'
import type { SessionId } from '@deepseek-ai/dsh-session/types'
import { isTypertOwnedValue } from '@deepseek-ai/dsh-typert-protocol'
import { LiaisonError } from '../src/errors.ts'
import { createInteractionContexts, InteractionBroker } from '../src/interactions.ts'

function broker(binding: Record<string, string> = { 'session-1': 'd1' }, signalIds = { next: 0 }): InteractionBroker {
  return new InteractionBroker({
    bindingOfSession: sessionId => binding[sessionId],
    now: () => 100,
    newId: () => `id-${String(++signalIds.next)}`,
  })
}

describe('bound interaction routing', () => {
  it('resolves a Client context only for bound delegations', () => {
    const contexts = createInteractionContexts(new Context(), sessionId => (sessionId === 'session-1' ? 'd1' : undefined))
    const sessionId = brandString<SessionId>('session-1')
    const scope = contexts.adapter.resolve(sessionId)
    assert.ok(scope !== undefined && !isTypertOwnedValue(scope))
    assert.equal(contexts.adapter.identity(scope), sessionId)
    assert.equal(contexts.adapter.resolve(brandString<SessionId>('session-user')), undefined)
    assert.equal(contexts.adapter.resolve(sessionId), scope)
  })
})

describe('interaction broker', () => {
  it('keeps an approval pending until the operator approves it', async () => {
    const interactions = broker()
    const outcome = interactions.offerApproval('session-1', { toolName: 'shell', reason: 'write' })
    const [pending] = interactions.list('d1')
    assert.equal(pending?.kind, 'approval')
    assert.equal(pending?.toolName, 'shell')
    assert.equal(interactions.has(pending?.id ?? ''), true)
    interactions.approve(pending?.id ?? '')
    assert.equal(await outcome, 'allowed-once')
    assert.equal(interactions.list().length, 0)
  })

  it('rejects one approval with the closed rejected outcome', async () => {
    const interactions = broker()
    const outcome = interactions.offerApproval('session-1', { toolName: 'shell' })
    interactions.reject('interaction-id-1')
    assert.equal(await outcome, 'rejected')
  })

  it('answers only with a complete batch and removes settled questions', async () => {
    const interactions = broker()
    const outcome = interactions.offerQuestion('session-1', {
      questions: [{ id: 'q1', question: 'Proceed?' }],
      wait: { callId: brandString<ToolCallId>('call-1') },
    })
    const [pending] = interactions.list()
    assert.equal(pending?.callId, 'call-1')
    assert.throws(
      () => interactions.answer({ interactionId: pending?.id, answers: [] }),
      (error: unknown) => error instanceof LiaisonError && error.code === 'usage',
    )
    const settled = interactions.answer({
      interactionId: pending?.id,
      answers: [{ id: 'q1', selected: ['yes'] }],
    })
    assert.equal(settled?.id, pending?.id)
    assert.deepEqual(await outcome, { answers: [{ id: 'q1', selected: ['yes'] }] })
    assert.equal(interactions.list().length, 0)
  })

  it('clears a pending request when its delivery is cancelled', async () => {
    const interactions = broker()
    const abort = new AbortController()
    const outcome = interactions.offerQuestion('session-1', {
      questions: [{ id: 'q1', question: 'Proceed?' }],
      signal: abort.signal,
    })
    assert.equal(interactions.list().length, 1)
    abort.abort(new Error('browser answered'))
    await assert.rejects(outcome, /browser answered/)
    assert.equal(interactions.list().length, 0)
  })

  it('does not invent an interaction for an unbound session', async () => {
    const interactions = broker()
    await assert.rejects(
      interactions.offerApproval('session-user', { toolName: 'shell' }),
      (error: unknown) => error instanceof LiaisonError && error.code === 'not-found',
    )
    assert.equal(interactions.list().length, 0)
  })
})
