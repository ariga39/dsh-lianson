/**
 * Passive, contiguous session log reads and structured projections.
 *
 * `session.events` paginates backwards with a message budget, so one page is
 * not a dense sequence range. {@link EventReader.readRange} pins the tail
 * cursor, walks older pages, and rejects gaps, duplicates, disorder, moved
 * cursors and non-advancing pages instead of returning a silently incomplete
 * range.
 */

import { LiaisonError, remoteCodeOf } from './errors.mjs'

const MAX_PAGES = 10_000

function pageEvents(page) {
  return (page?.records ?? []).map(record => record.event)
}

function sequenceGap(sessionId, details) {
  return new LiaisonError(
    'events/sequence-gap',
    `session "${sessionId}" history is not contiguous: expected seq ${String(details.expectedSeq)}, `
      + `saw ${String(details.actualSeq)} (${details.kind})`,
    { sessionId, ...details },
  )
}

function assertPage(page, sessionId) {
  if (page === null || typeof page !== 'object'
    || !Number.isSafeInteger(page.cursor) || !Array.isArray(page.records)
    || typeof page.hasMore !== 'boolean') {
    throw new LiaisonError(
      'events/page-contract',
      `session "${sessionId}" returned an invalid session.events page`,
      { sessionId, cursor: page?.cursor ?? null },
    )
  }
}

function assertChronological(events, { sessionId, expectedFrom, throughSeq }) {
  for (let index = 0; index < events.length; index++) {
    const event = events[index]
    if (!Number.isSafeInteger(event?.seq)) {
      throw new LiaisonError(
        'events/page-contract',
        `session "${sessionId}" returned a record without an integer seq`,
        { sessionId, index },
      )
    }
    if (index === 0) continue
    const previous = events[index - 1]
    if (event.seq === previous.seq) {
      throw sequenceGap(sessionId, {
        kind: 'duplicate',
        expectedSeq: previous.seq + 1,
        actualSeq: event.seq,
        afterSeq: expectedFrom,
        throughSeq,
      })
    }
    if (event.seq < previous.seq) {
      throw sequenceGap(sessionId, {
        kind: 'order',
        expectedSeq: previous.seq + 1,
        actualSeq: event.seq,
        afterSeq: expectedFrom,
        throughSeq,
      })
    }
  }
}

/**
 * Read complete session history over a pinned cursor without activating it.
 */
export class EventReader {
  #adapter

  /** @param {{ adapter: object }} options - Host Remote adapter exposing `sessionEvents`. */
  constructor({ adapter }) {
    if (adapter === null || typeof adapter !== 'object'
      || typeof adapter.sessionEvents !== 'function') {
      throw new TypeError('EventReader requires an adapter with sessionEvents')
    }
    this.#adapter = adapter
  }

  /**
   * Read the dense sequence range `(afterSeq, throughSeq]`.
   *
   * The tail page's cursor is pinned for every older page. The final range is
   * checked against `[afterSeq + 1 .. pinnedCursor]` with step 1.
   * @param {{ sessionId: string, address?: object, afterSeq?: number, throughSeq?: number, pageSize?: number }} request - session, optional public address and range.
   * @returns {{ sessionId: string, cursor: number, pinnedCursor: number, afterSeq: number, events: object[], pagesRead: number }} the contiguous range.
   * @throws {LiaisonError} `events/sequence-gap`, `events/pagination-stalled`, `events/cursor-moved` or `events/page-contract`.
   */
  async readRange({ sessionId, address, afterSeq = -1, throughSeq, pageSize }) {
    if (typeof sessionId !== 'string' || sessionId === '') throw new TypeError('sessionId is required')
    if (address !== undefined && (address === null || typeof address !== 'object'
      || typeof address.kind !== 'string')) {
      throw new TypeError('address must be a SessionAddress')
    }
    if (address?.kind === 'subagent' && address.childSessionId !== sessionId) {
      throw new TypeError('subagent address must name sessionId as its child')
    }
    if (!Number.isSafeInteger(afterSeq) || afterSeq < -1) {
      throw new TypeError('afterSeq must be an integer greater than or equal to -1')
    }
    if (throughSeq !== undefined && (!Number.isSafeInteger(throughSeq) || throughSeq < -1)) {
      throw new TypeError('throughSeq must be an integer greater than or equal to -1')
    }
    if (pageSize !== undefined && (!Number.isSafeInteger(pageSize) || pageSize <= 0)) {
      throw new TypeError('pageSize must be a positive safe integer')
    }

    const target = address ?? { kind: 'session', sessionId }
    const pageRequest = pageSize === undefined ? {} : { maxMessages: pageSize }
    const first = await this.#adapter.sessionEvents({
      address: target,
      ...(throughSeq === undefined ? {} : { throughSeq }),
      ...pageRequest,
    })
    assertPage(first, sessionId)
    const pinnedCursor = first.cursor
    let loaded = pageEvents(first)
    assertChronological(loaded, { sessionId, expectedFrom: afterSeq, throughSeq: pinnedCursor })
    let page = first
    let pagesRead = 1

    while (page.hasMore) {
      const earliest = loaded[0]?.seq
      if (earliest === undefined) {
        throw new LiaisonError(
          'events/pagination-stalled',
          `session "${sessionId}" reported more history but returned no records`,
          { sessionId, afterSeq, throughSeq: pinnedCursor, beforeSeq: null, actualSeq: null },
        )
      }
      // Older events cannot help once the earliest loaded event is the first
      // event in the requested range; stop paginating.
      if (earliest <= afterSeq + 1) break
      if (pagesRead > MAX_PAGES) {
        throw new LiaisonError(
          'events/pagination-stalled',
          `session "${sessionId}" pagination exceeded ${String(MAX_PAGES)} pages`,
          { sessionId, afterSeq, throughSeq: pinnedCursor, beforeSeq: earliest, actualSeq: null },
        )
      }
      const beforeSeq = earliest
      const older = await this.#adapter.sessionEvents({
        address: target,
        throughSeq: pinnedCursor,
        beforeSeq,
        ...pageRequest,
      })
      assertPage(older, sessionId)
      if (older.cursor !== pinnedCursor) {
        throw new LiaisonError(
          'events/cursor-moved',
          `session "${sessionId}" moved its history cursor from ${String(pinnedCursor)} to `
            + `${String(older.cursor)} during pagination`,
          { sessionId, afterSeq, pinnedCursor, actualCursor: older.cursor, beforeSeq },
        )
      }
      const olderEvents = pageEvents(older)
      assertChronological(olderEvents, { sessionId, expectedFrom: afterSeq, throughSeq: pinnedCursor })
      const overlap = olderEvents.find(event => event.seq >= beforeSeq)
      if (overlap !== undefined) {
        throw new LiaisonError(
          'events/pagination-stalled',
          `session "${sessionId}" pagination did not advance before seq ${String(beforeSeq)}: `
            + `the older page still contained seq ${String(overlap.seq)}`,
          { sessionId, afterSeq, throughSeq: pinnedCursor, beforeSeq, actualSeq: overlap.seq },
        )
      }
      if (olderEvents.length === 0) {
        throw new LiaisonError(
          'events/pagination-stalled',
          `session "${sessionId}" pagination returned an empty older page before seq ${String(beforeSeq)}`,
          { sessionId, afterSeq, throughSeq: pinnedCursor, beforeSeq, actualSeq: null },
        )
      }
      loaded = [...olderEvents, ...loaded]
      page = older
      pagesRead += 1
    }

    const events = loaded.filter(event => event.seq > afterSeq && event.seq <= pinnedCursor)
    let expected = afterSeq + 1
    for (const event of events) {
      if (event.seq !== expected) {
        throw sequenceGap(sessionId, {
          kind: event.seq < expected ? 'duplicate' : 'missing',
          expectedSeq: expected,
          actualSeq: event.seq,
          afterSeq,
          throughSeq: pinnedCursor,
        })
      }
      expected += 1
    }
    if (expected !== pinnedCursor + 1) {
      throw sequenceGap(sessionId, {
        kind: 'missing',
        expectedSeq: expected,
        actualSeq: pinnedCursor,
        afterSeq,
        throughSeq: pinnedCursor,
      })
    }

    return {
      sessionId,
      cursor: pinnedCursor,
      pinnedCursor,
      afterSeq,
      events,
      pagesRead,
    }
  }
}

function textOf(content) {
  if (!Array.isArray(content)) return ''
  return content
    .filter(block => block?.type === 'text' && typeof block.text === 'string')
    .map(block => block.text)
    .join('')
}

/**
 * Extract chronological user and assistant messages.
 * @param {readonly object[]} events - contiguous wire events.
 * @returns {{ seq: number, time: number, role: string, content: object[], text: string, source: object|null, turn?: number, step?: number }[]} messages.
 */
export function extractMessages(events) {
  const messages = []
  for (const event of events) {
    if (event.type === 'user/message') {
      const content = Array.isArray(event.data?.content) ? event.data.content : []
      messages.push({
        seq: event.seq,
        time: event.time,
        role: 'user',
        content,
        text: textOf(content),
        source: event.data?.source ?? null,
      })
    } else if (event.type === 'assistant/message') {
      const payload = event.data?.message ?? {}
      const content = Array.isArray(payload.content) ? payload.content : []
      messages.push({
        seq: event.seq,
        time: event.time,
        role: 'assistant',
        content,
        text: textOf(content),
        source: payload.source ?? null,
        turn: event.data?.turn,
        step: event.data?.step,
      })
    }
  }
  return messages
}

function parseArguments(value) {
  if (typeof value !== 'string') return value ?? null
  try {
    return JSON.parse(value)
  } catch {
    return null
  }
}

/**
 * Pair `tool/call` events with their `tool/result` by call id.
 *
 * In-flight calls keep `result: undefined`; results without a preceding call
 * are reported separately instead of being dropped.
 * @param {readonly object[]} events - contiguous wire events.
 * @returns {{ calls: object[], unpairedResults: object[] }} tool activity.
 */
export function extractToolActivity(events) {
  const calls = []
  const byCallId = new Map()
  const unpairedResults = []

  for (const event of events) {
    if (event.type === 'tool/call') {
      const call = {
        seq: event.seq,
        time: event.time,
        callId: event.data?.callId,
        name: event.data?.name,
        arguments: event.data?.arguments ?? null,
        input: parseArguments(event.data?.arguments),
        turn: event.data?.turn,
        step: event.data?.step,
        result: undefined,
      }
      calls.push(call)
      byCallId.set(call.callId, call)
      continue
    }
    if (event.type !== 'tool/result') continue
    const message = event.data?.message ?? {}
    const callId = message.toolCallId ?? message.source?.callId
    const result = {
      seq: event.seq,
      time: event.time,
      callId,
      isError: message.isError === true,
      output: message.content ?? null,
      error: event.data?.error ?? null,
      meta: event.data?.meta ?? null,
    }
    const call = byCallId.get(callId)
    if (call === undefined) unpairedResults.push(result)
    else call.result = result
  }
  return { calls, unpairedResults }
}

/**
 * Extract persisted request routes.
 * @param {readonly object[]} events - contiguous wire events.
 * @returns {{ seq: number, time: number, config: object }}[] request headers.
 * @throws {LiaisonError} `events/header-invalid` when a header lacks a route.
 */
export function extractRequestHeaders(events) {
  const headers = []
  for (const event of events) {
    if (event.type !== 'request/header') continue
    const config = event.data?.header?.config
    if (config === null || typeof config !== 'object'
      || typeof config.provider !== 'string' || typeof config.model !== 'string') {
      throw new LiaisonError(
        'events/header-invalid',
        `request/header at seq ${String(event.seq)} has no provider/model config`,
        { seq: event.seq },
      )
    }
    headers.push({
      seq: event.seq,
      time: event.time,
      config: {
        provider: config.provider,
        model: config.model,
        ...(config.reasoningEffort === undefined ? {} : { reasoningEffort: config.reasoningEffort }),
      },
    })
  }
  return headers
}

function projectionValues(projections) {
  if (projections === null || projections === undefined) return undefined
  if (projections.kind !== 'sequenced' && projections.kind !== 'migration-required') return undefined
  return projections.values ?? undefined
}

function emptySubagentStatus(reason, errorCode = null) {
  return {
    available: false,
    reason,
    errorCode,
    asOfSeq: null,
    complete: true,
    catalog: [],
    descendants: [],
    localSubagents: [],
    settledLocalSubagents: [],
    activeSubagents: [],
    unknownSubagents: [],
    externalSubagents: [],
    unavailableSubagents: [],
  }
}

function subagentIdentity(entry, parentSessionId, depth) {
  return {
    id: entry.id,
    mode: entry.mode ?? 'unknown',
    createdAt: entry.createdAt ?? null,
    ...(entry.label === undefined ? {} : { label: entry.label }),
    ...(entry.name === undefined ? {} : { name: entry.name }),
    parentSessionId,
    depth,
  }
}

/**
 * Read the observable direct-child catalog and every local descendant.
 *
 * Local descendants are walked recursively through their own `subagentCatalog`
 * projection because the root catalog names only direct children. `unknown` and
 * `external` rows have no readable local session; they are reported with
 * `active: null` / `settled: null` and never probed. A local child whose state
 * or catalog cannot be read is reported as `unavailable`.
 * @param {object} adapter - Host Remote adapter with `sessionProjections` and `sessionState`.
 * @param {string} sessionId - parent session.
 * @param {{ maxNodes?: number, recursive?: boolean }} [options] - traversal ceiling and descendant walk.
 * @returns the direct catalog, flattened descendants and honest per-node observation.
 */
export async function readSubagentStatus(adapter, sessionId, { maxNodes = 256, recursive = true } = {}) {
  let projections
  try {
    projections = await adapter.sessionProjections({ sessionId })
  } catch (error) {
    return emptySubagentStatus('projections-error', remoteCodeOf(error) ?? null)
  }
  const values = projectionValues(projections)
  if (values === undefined) return emptySubagentStatus('projections-unavailable')
  if (!Array.isArray(values.subagentCatalog)) {
    return emptySubagentStatus('subagent-catalog-unavailable')
  }

  const descendants = []
  const seen = new Set([sessionId])
  let complete = true

  const observe = async (parentSessionId, entries, depth) => {
    for (const entry of entries) {
      if (descendants.length >= maxNodes) {
        complete = false
        return
      }
      const identity = subagentIdentity(entry, parentSessionId, depth)
      if (identity.mode !== 'one-shot' && identity.mode !== 'continuable') {
        descendants.push({
          ...identity,
          local: false,
          observation: identity.mode === 'external' ? 'external' : 'unknown-mode',
          active: null,
          settled: null,
          live: null,
          running: null,
          pendingInput: null,
          timing: null,
        })
        continue
      }
      if (seen.has(identity.id)) {
        descendants.push({
          ...identity,
          local: true,
          observation: 'unavailable',
          errorCode: 'subagent/cycle',
          active: null,
          settled: null,
          live: null,
          running: null,
          pendingInput: null,
          timing: null,
        })
        continue
      }
      seen.add(identity.id)

      let state
      try {
        state = await adapter.sessionState({ sessionId: identity.id })
      } catch (error) {
        descendants.push({
          ...identity,
          local: true,
          observation: 'unavailable',
          errorCode: remoteCodeOf(error) ?? null,
          active: null,
          settled: null,
          live: null,
          running: null,
          pendingInput: null,
          timing: null,
        })
        continue
      }

      let timing = null
      let timingErrorCode = null
      let children = null
      try {
        const childProjections = await adapter.sessionProjections({ sessionId: identity.id })
        const childValues = projectionValues(childProjections)
        timing = childValues?.subagentTiming ?? null
        if (Array.isArray(childValues?.subagentCatalog)) {
          children = childValues.subagentCatalog
        }
      } catch (error) {
        timingErrorCode = remoteCodeOf(error) ?? null
      }

      const pendingInput = Array.isArray(state?.pendingInput) ? state.pendingInput : []
      const queueLength = Array.isArray(state?.queue) ? state.queue.length : 0
      const running = state?.running === true
      const live = state?.active === true
      const node = {
        ...identity,
        local: true,
        observation: 'state',
        active: running || queueLength > 0,
        settled: !running && queueLength === 0 && pendingInput.length === 0 && live === false,
        live,
        running,
        queueLength,
        pendingInput,
        cursor: state?.cursor ?? null,
        ...(timingErrorCode === null ? {} : { timingErrorCode }),
        ...(children === null ? { catalogUnavailable: true } : {}),
        timing,
      }
      descendants.push(node)
      if (recursive && children !== null && children.length > 0) {
        await observe(identity.id, children, depth + 1)
      }
    }
  }

  await observe(sessionId, values.subagentCatalog, 0)
  const catalog = descendants.filter(entry => entry.depth === 0)
  const localSubagents = descendants.filter(entry => entry.local === true)
  return {
    available: true,
    reason: null,
    errorCode: null,
    asOfSeq: Number.isSafeInteger(projections.asOfSeq) ? projections.asOfSeq : null,
    complete,
    catalog,
    descendants,
    localSubagents,
    settledLocalSubagents: localSubagents.filter(entry =>
      entry.observation === 'state' && entry.settled === true),
    activeSubagents: localSubagents.filter(entry => entry.active === true),
    unknownSubagents: descendants.filter(entry => entry.mode === 'unknown'),
    externalSubagents: descendants.filter(entry => entry.mode === 'external'),
    unavailableSubagents: localSubagents.filter(entry =>
      entry.observation === 'unavailable' || entry.catalogUnavailable === true),
  }
}
