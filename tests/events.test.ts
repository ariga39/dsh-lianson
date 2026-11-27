import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import type { SessionWireEvent } from '@deepseek-ai/dsh-api-session-controller/types'
import { EventReader } from '../src/events.ts'
import { LiaisonError } from '../src/errors.ts'
import type { RemoteEventPage, RemoteEventPageRequest } from '../src/remote.ts'

function event(seq: number): SessionWireEvent {
  return { type: 'fixture/event', seq, time: seq, data: { seq } }
}

function page(cursor: number, seqs: readonly number[], hasMore = false): RemoteEventPage {
  return { cursor, events: seqs.map(event), hasMore }
}

describe('event range pagination', () => {
  it('walks any number of older pages back to the requested lower bound', async () => {
    const requests: RemoteEventPageRequest[] = []
    const pages = [page(10, [5, 6, 7, 8, 9, 10], true), page(10, [0, 1, 2, 3, 4], false)]
    const reader = new EventReader({
      readEventPage: async (_sessionId, request) => {
        requests.push(request)
        const next = pages.shift()
        if (next === undefined) throw new Error('unscripted page')
        return next
      },
    })
    const range = await reader.readRange('session-1', { afterSeq: -1, pageSize: 2 })
    assert.deepEqual(range.events.map(item => item.seq), [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    assert.equal(range.pinnedCursor, 10)
    assert.equal(range.pagesRead, 2)
    assert.deepEqual(requests, [
      { maxMessages: 2 },
      { throughSeq: 10, beforeSeq: 5, maxMessages: 2 },
    ])
  })

  it('fails explicitly on a gap instead of returning a partial range', async () => {
    const reader = new EventReader({ readEventPage: async () => page(4, [0, 1, 3, 4]) })
    await assert.rejects(
      reader.readRange('session-gap'),
      (error: unknown) => error instanceof LiaisonError
        && error.code === 'events/sequence-gap'
        && error.details?.expectedSeq === 2
        && error.details?.actualSeq === 3,
    )
  })

  it('rejects disordered and duplicate pages', async () => {
    await assert.rejects(
      new EventReader({ readEventPage: async () => page(2, [0, 2, 1]) }).readRange('session-order'),
      (error: unknown) => error instanceof LiaisonError && error.code === 'events/sequence-gap',
    )
    await assert.rejects(
      new EventReader({ readEventPage: async () => page(2, [0, 1, 1, 2]) }).readRange('session-duplicate'),
      (error: unknown) => error instanceof LiaisonError && error.code === 'events/sequence-gap',
    )
  })

  it('does not stop at the old twelve-page history limit', async () => {
    // One event per page, newest first. The previous fixed twelve-page loop
    // would miss seq 0; the range walker must keep going to the bound.
    const total = 18
    const pages: RemoteEventPage[] = []
    for (let seq = total - 1; seq >= 0; seq -= 1) {
      pages.push(page(total - 1, [seq], seq > 0))
    }
    const reader = new EventReader({
      readEventPage: async () => {
        const next = pages.shift()
        if (next === undefined) throw new Error('unscripted page')
        return next
      },
    })
    const range = await reader.readRange('session-long', { afterSeq: -1 })
    assert.equal(range.events.length, total)
    assert.equal(range.events[0]?.seq, 0)
    assert.equal(range.pagesRead, total)
  })

  it('reads beyond-head cursors as empty without inventing events', async () => {
    const reader = new EventReader({ readEventPage: async () => page(3, [0, 1, 2, 3]) })
    const range = await reader.readRange('session-caught-up', { afterSeq: 3 })
    assert.deepEqual(range.events, [])
    assert.equal(range.cursor, 3)
  })
})
