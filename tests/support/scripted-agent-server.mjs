import { randomUUID } from 'node:crypto'
import { createServer } from 'node:http'

function sse(response, payload) {
  response.write(`event: ${payload.type}\ndata: ${JSON.stringify(payload)}\n\n`)
}

function messageStart(response, model) {
  sse(response, {
    type: 'message_start',
    message: {
      id: 'mock-message',
      type: 'message',
      role: 'assistant',
      model,
      content: [],
      usage: { input_tokens: 1, output_tokens: 0 },
    },
  })
}

function finalMessage(response, model, text) {
  messageStart(response, model)
  sse(response, { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } })
  sse(response, { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text } })
  sse(response, { type: 'content_block_stop', index: 0 })
  sse(response, { type: 'message_delta', delta: { stop_reason: 'end_turn', stop_sequence: null }, usage: { output_tokens: 2 } })
  sse(response, { type: 'message_stop' })
  response.end()
}

function toolCall(response, { model, name, input }) {
  const id = `mock-${randomUUID()}`
  messageStart(response, model)
  sse(response, { type: 'content_block_start', index: 0, content_block: { type: 'tool_use', id, name, input: {} } })
  sse(response, { type: 'content_block_delta', index: 0, delta: { type: 'input_json_delta', partial_json: JSON.stringify(input) } })
  sse(response, { type: 'content_block_stop', index: 0 })
  sse(response, { type: 'message_delta', delta: { stop_reason: 'tool_use', stop_sequence: null }, usage: { output_tokens: 1 } })
  sse(response, { type: 'message_stop' })
  response.end()
}

/**
 * Scripted Messages-compatible model endpoint for slice 2.
 * Prompts select a tool action with a marker; the follow-up request carrying the
 * tool result receives a final text reply.
 */
export async function startScriptedAgentServer() {
  const requests = []
  const errors = []
  const server = createServer(async (request, response) => {
    if (!(request.url ?? '').endsWith('/v1/messages')) {
      response.writeHead(404).end()
      return
    }
    try {
      const chunks = []
      for await (const chunk of request) chunks.push(chunk)
      const body = JSON.parse(Buffer.concat(chunks).toString('utf8'))
      requests.push({ url: request.url, headers: request.headers, body })
      const serialized = JSON.stringify(body)
      const lastMessage = JSON.stringify(body.messages?.at(-1) ?? {})
      if (lastMessage.includes('"tool_result"')) {
        response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' })
        finalMessage(response, body.model, 'SLICE2_FINAL_DONE')
        return
      }
      const markers = [...serialized.matchAll(/WRITE_(LOCAL|CROSS) ([^"\\\s]+)/gu)]
      const marker = markers.at(-1)
      if (marker?.[1] === 'LOCAL') {
        response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' })
        toolCall(response, {
          model: body.model,
          name: 'write',
          input: { file_path: marker[2], content: `written by ${body.model}\n` },
        })
        return
      }
      if (marker?.[1] === 'CROSS') {
        response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' })
        toolCall(response, {
          model: body.model,
          name: 'write',
          input: { file_path: marker[2], content: 'cross-worktree write\n' },
        })
        return
      }
      response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' })
      finalMessage(response, body.model, 'SLICE2_FINAL_DONE')
    } catch (error) {
      errors.push(error)
      response.destroy(error instanceof Error ? error : undefined)
    }
  })

  await new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('mock server has no port')

  return {
    baseURL: `http://127.0.0.1:${String(address.port)}`,
    requests,
    errors,
    close: () => new Promise((resolve, reject) => {
      server.closeAllConnections()
      server.close(error => error === undefined ? resolve() : reject(error))
    }),
  }
}
