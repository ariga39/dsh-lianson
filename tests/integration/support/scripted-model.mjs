/**
 * Keyless scripted model endpoint used by the real Host. It speaks the same
 * Anthropic-shaped SSE stream the shipped DeepSeek provider consumes, so the
 * tests exercise real session, tool, approval, and question mechanics without
 * a model credential.
 */

import { createServer } from 'node:http'

function sse(response, payload) {
  if (response.destroyed) return
  response.write(`event: ${payload.type}\ndata: ${JSON.stringify(payload)}\n\n`)
}

function messageStart(response, model) {
  sse(response, {
    type: 'message_start',
    message: {
      id: 'mock-message', type: 'message', role: 'assistant', model,
      content: [], usage: { input_tokens: 1, output_tokens: 0 },
    },
  })
}

function messageDelta(response, stopReason) {
  sse(response, {
    type: 'message_delta',
    delta: { stop_reason: stopReason, stop_sequence: null },
    usage: { output_tokens: 1 },
  })
  sse(response, { type: 'message_stop' })
}

function abortPromise(signal) {
  if (signal.aborted) return Promise.resolve()
  return new Promise((resolve) => signal.addEventListener('abort', resolve, { once: true }))
}

async function writePlan(response, model, plan, signal) {
  if (plan.kind === 'hold') {
    await abortPromise(signal)
    return
  }
  response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' })
  messageStart(response, model)
  if (plan.kind === 'tool') {
    let index = 0
    if (plan.text !== undefined && plan.text.length > 0) {
      sse(response, { type: 'content_block_start', index, content_block: { type: 'text', text: '' } })
      sse(response, { type: 'content_block_delta', index, delta: { type: 'text_delta', text: plan.text } })
      sse(response, { type: 'content_block_stop', index })
      index += 1
    }
    sse(response, {
      type: 'content_block_start',
      index,
      content_block: { type: 'tool_use', id: plan.id ?? `mock-tool-${String(Date.now())}`, name: plan.name, input: {} },
    })
    sse(response, {
      type: 'content_block_delta',
      index,
      delta: { type: 'input_json_delta', partial_json: JSON.stringify(plan.input) },
    })
    sse(response, { type: 'content_block_stop', index })
    messageDelta(response, 'tool_use')
    response.end()
    return
  }
  sse(response, { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } })
  for (const part of plan.parts ?? [{ text: plan.text ?? '' }]) {
    if (part.gate !== undefined) await part.gate
    if (signal.aborted || response.destroyed) return
    if (part.text !== undefined && part.text.length > 0) {
      sse(response, { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: part.text } })
    }
  }
  sse(response, { type: 'content_block_stop', index: 0 })
  messageDelta(response, 'end_turn')
  response.end()
}

/**
 * Start the mock endpoint. Set `server.handler` before the first prompt; the
 * default answers with `DEFAULT_SENTINEL`.
 * @returns {Promise<{baseURL: string, requests: object[], errors: unknown[], setHandler: (handler: Function) => void, close: () => Promise<void>}>}
 */
export async function startScriptedModel() {
  const requests = []
  const errors = []
  let handler = async () => ({ kind: 'text', text: 'DEFAULT_SENTINEL' })

  const server = createServer(async (request, response) => {
    if (!(request.url ?? '').endsWith('/v1/messages')) {
      response.writeHead(404).end()
      return
    }
    const controller = new AbortController()
    response.once('close', () => controller.abort())
    try {
      const chunks = []
      for await (const chunk of request) chunks.push(chunk)
      if (controller.signal.aborted) return
      const body = JSON.parse(Buffer.concat(chunks).toString('utf8'))
      const entry = {
        url: request.url,
        headers: request.headers,
        body,
        text: JSON.stringify(body),
        number: requests.length,
      }
      requests.push(entry)
      const plan = await handler({ ...entry, signal: controller.signal })
      if (plan === undefined) {
        if (!response.headersSent && !response.destroyed) response.writeHead(500).end('script returned no plan')
        return
      }
      await writePlan(response, body.model, plan, controller.signal)
    } catch (error) {
      errors.push(error)
      if (!response.destroyed) response.destroy(error instanceof Error ? error : undefined)
    }
  })

  await new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('scripted model has no port')

  return {
    baseURL: `http://127.0.0.1:${String(address.port)}`,
    requests,
    errors,
    setHandler: (next) => { handler = next },
    close: () => new Promise((resolve, reject) => {
      server.closeAllConnections()
      server.close((error) => error === undefined ? resolve() : reject(error))
    }),
  }
}
