/**
 * Dense Session-history reads over the Host's backwards `session.events`
 * pagination. One page is message-bounded, not sequence-bounded, so a range
 * read pins the tail cursor, walks older pages, and verifies continuity before
 * handing the range back.
 */

import type { SessionWireEvent } from '@deepseek-ai/dsh-api-session-controller/types'
import { LiaisonError } from './errors.ts'
import type { RemoteEventPage, RemoteEventPageRequest } from './remote.ts'

/** Maximum older pages read for one range; guards against an upstream loop. */
const MAX_PAGES = 10_000

/** One range request over durable Session events. */
export interface EventRangeRequest {
  /** Exclusive lower bound; `-1` includes the first event. */
  readonly afterSeq?: number
  /** Inclusive pinned cut; omission selects the current log end. */
  readonly throughSeq?: number
  /** Host message budget for each page. */
  readonly pageSize?: number
  /**
   * Reject a non-dense range. History commands default to true; model
   * verification reads sparse unit fixtures non-strictly.
   */
  readonly requireContiguous?: boolean
}

/** One verified range of durable events. */
export interface EventRange {
  readonly sessionId: string
  readonly afterSeq: number
  readonly cursor: number
  readonly pinnedCursor: number
  readonly events: readonly SessionWireEvent[]
  readonly pagesRead: number
}

/** The page reader the range walker needs. */
export interface EventPageSource {
  readEventPage(sessionId: string, request: RemoteEventPageRequest): Promise<RemoteEventPage>
}

/** Read a dense or sorted sequence range across any number of Host pages. */
export class EventReader {
  private readonly source: EventPageSource

  /** @param source - remote page reader. */
  constructor(source: EventPageSource) {
    this.source = source
  }

  /**
   * Read `(afterSeq, throughSeq]`, pinning the first page's cursor.
   * @param sessionId - durable Session identity.
   * @param request - range and page bounds.
   * @returns the chronological range and the pinned cut.
   */
  async readRange(sessionId: string, request: EventRangeRequest = {}): Promise<EventRange> {
    const afterSeq = request.afterSeq ?? -1
    const throughSeq = request.throughSeq
    const pageSize = request.pageSize
    const strict = request.requireContiguous ?? true
    if (!Number.isSafeInteger(afterSeq) || afterSeq < -1) {
      throw new LiaisonError('usage', 'afterSeq must be an integer greater than or equal to -1', { afterSeq })
    }
    if (throughSeq !== undefined && (!Number.isSafeInteger(throughSeq) || throughSeq < -1)) {
      throw new LiaisonError('usage', 'throughSeq must be an integer greater than or equal to -1', { throughSeq })
    }
    if (throughSeq !== undefined && afterSeq > throughSeq) {
      throw new LiaisonError('events/bad-range', `afterSeq ${String(afterSeq)} is beyond throughSeq ${String(throughSeq)}`, {
        sessionId,
        afterSeq,
        throughSeq,
      })
    }
    if (pageSize !== undefined && (!Number.isSafeInteger(pageSize) || pageSize <= 0)) {
      throw new LiaisonError('usage', 'pageSize must be a positive safe integer', { pageSize })
    }

    const first = await this.source.readEventPage(sessionId, {
      ...(throughSeq === undefined ? {} : { throughSeq }),
      ...(pageSize === undefined ? {} : { maxMessages: pageSize }),
    })
    assertPage(first, sessionId)
    const pinnedCursor = first.cursor
    if (pinnedCursor <= afterSeq) {
      return { sessionId, afterSeq, cursor: pinnedCursor, pinnedCursor, events: [], pagesRead: 1 }
    }

    let oldest = oldestSeq(first.events)
    let loaded = [...first.events]
    let hasMore = first.hasMore ?? false
    let pagesRead = 1
    assertChronological(loaded, sessionId, strict)

    while (hasMore) {
      if (oldest === undefined) {
        throw stalled(sessionId, afterSeq, pinnedCursor, null)
      }
      // Older pages cannot add anything once the first needed event is loaded.
      if (oldest <= afterSeq + 1) break
      if (pagesRead >= MAX_PAGES) throw stalled(sessionId, afterSeq, pinnedCursor, null)
      const page = await this.source.readEventPage(sessionId, {
        throughSeq: pinnedCursor,
        beforeSeq: oldest,
        ...(pageSize === undefined ? {} : { maxMessages: pageSize }),
      })
      assertPage(page, sessionId)
      if (page.cursor !== pinnedCursor) {
        throw new LiaisonError('events/cursor-moved', `session "${sessionId}" moved its history cursor from ${pinnedCursor} to ${page.cursor} during pagination`, {
          sessionId,
          pinnedCursor,
          actualCursor: page.cursor,
          beforeSeq: oldest,
        })
      }
      const olderEvents = page.events
      assertChronological(olderEvents, sessionId, strict)
      const overlap = olderEvents.find(event => event.seq >= oldest!)
      if (overlap !== undefined) {
        throw stalled(sessionId, afterSeq, pinnedCursor, overlap.seq)
      }
      if (olderEvents.length === 0 && page.hasMore === true) {
        throw stalled(sessionId, afterSeq, pinnedCursor, null)
      }
      loaded = [...olderEvents, ...loaded]
      oldest = oldestSeq(olderEvents) ?? oldest
      hasMore = page.hasMore ?? false
      pagesRead += 1
    }

    const events = loaded
      .filter(event => event.seq > afterSeq && event.seq <= pinnedCursor)
      .sort((left, right) => left.seq - right.seq)
    if (strict) assertDense(events, sessionId, afterSeq, pinnedCursor)
    return { sessionId, afterSeq, cursor: pinnedCursor, pinnedCursor, events, pagesRead }
  }
}

function oldestSeq(events: readonly SessionWireEvent[]): number | undefined {
  let oldest: number | undefined
  for (const event of events) {
    if (oldest === undefined || event.seq < oldest) oldest = event.seq
  }
  return oldest
}

function assertPage(page: RemoteEventPage, sessionId: string): void {
  if (page === null || typeof page !== 'object'
    || !Number.isSafeInteger(page.cursor) || !Array.isArray(page.events)
    || (page.hasMore !== undefined && typeof page.hasMore !== 'boolean')) {
    throw new LiaisonError('events/page-contract', `session "${sessionId}" returned an invalid events page`, { sessionId })
  }
}

function assertChronological(events: readonly SessionWireEvent[], sessionId: string, strict: boolean): void {
  for (let index = 0; index < events.length; index += 1) {
    const event = events[index]
    if (event === undefined || !Number.isSafeInteger(event.seq)) {
      throw new LiaisonError('events/page-contract', `session "${sessionId}" returned an event without an integer seq`, { sessionId, index })
    }
    if (!strict || index === 0) continue
    const previous = events[index - 1]
    if (previous !== undefined && event.seq <= previous.seq) {
      throw new LiaisonError('events/sequence-gap', `session "${sessionId}" history is not ordered at seq ${event.seq}`, {
        sessionId,
        kind: event.seq === previous.seq ? 'duplicate' : 'order',
        expectedSeq: previous.seq + 1,
        actualSeq: event.seq,
      })
    }
  }
}

function assertDense(events: readonly SessionWireEvent[], sessionId: string, afterSeq: number, throughSeq: number): void {
  let expected = afterSeq + 1
  for (const event of events) {
    if (event.seq !== expected) {
      throw new LiaisonError('events/sequence-gap', `session "${sessionId}" history is not contiguous: expected seq ${expected}, saw ${event.seq}`, {
        sessionId,
        kind: event.seq < expected ? 'duplicate' : 'missing',
        expectedSeq: expected,
        actualSeq: event.seq,
        afterSeq,
        throughSeq,
      })
    }
    expected += 1
  }
  if (expected !== throughSeq + 1) {
    throw new LiaisonError('events/sequence-gap', `session "${sessionId}" history is not contiguous: expected seq ${expected}, saw ${throughSeq}`, {
      sessionId,
      kind: 'missing',
      expectedSeq: expected,
      actualSeq: throughSeq,
      afterSeq,
      throughSeq,
    })
  }
}

function stalled(sessionId: string, afterSeq: number, throughSeq: number, actualSeq: number | null): LiaisonError {
  return new LiaisonError('events/pagination-stalled', `session "${sessionId}" pagination did not advance`, {
    sessionId,
    afterSeq,
    throughSeq,
    actualSeq,
  })
}
