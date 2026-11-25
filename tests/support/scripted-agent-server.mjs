import { randomUUID } from 'node:crypto'
import { createServer } from 'node:http'

function sse(response, payload) {
  if (response.destroyed) return
  response.write(`event: ${payload.type}\ndata: ${JSON.stringify(payload)}\n\n`)
}

function delay(milliseconds) {
  return new Promise(resolve => setTimeout(resolve, milliseconds))
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

async function finalMessage(response, model, text, holdMs = 0) {
  messageStart(response, model)
  sse(response, { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } })
  if (holdMs > 0) {
    const chunks = Math.max(1, Math.min(8, Math.ceil(holdMs / 250)))
    const size = Math.max(1, Math.ceil(text.length / chunks))
    for (let index = 0; index < chunks; index++) {
      await delay(holdMs / chunks)
      if (response.destroyed) return
      const value = text.slice(index * size, (index + 1) * size)
      sse(response, { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: value } })
    }
  } else {
    sse(response, { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text } })
  }
  sse(response, { type: 'content_block_stop', index: 0 })
  sse(response, { type: 'message_delta', delta: { stop_reason: 'end_turn', stop_sequence: null }, usage: { output_tokens: 2 } })
  sse(response, { type: 'message_stop' })
  response.end()
}

async function toolCall(response, { model, name, input, holdMs = 0 }) {
  const id = `mock-${randomUUID()}`
  messageStart(response, model)
  sse(response, { type: 'content_block_start', index: 0, content_block: { type: 'tool_use', id, name, input: {} } })
  if (holdMs > 0) await delay(holdMs / 2)
  if (response.destroyed) return
  sse(response, { type: 'content_block_delta', index: 0, delta: { type: 'input_json_delta', partial_json: JSON.stringify(input) } })
  if (holdMs > 0) await delay(holdMs / 2)
  if (response.destroyed) return
  sse(response, { type: 'content_block_stop', index: 0 })
  sse(response, { type: 'message_delta', delta: { stop_reason: 'tool_use', stop_sequence: null }, usage: { output_tokens: 1 } })
  sse(response, { type: 'message_stop' })
  response.end()
}

function marker(lastMessage, name) {
  const match = new RegExp(`${name} (\\d+)`, 'u').exec(lastMessage)
  return match === null ? undefined : Number(match[1])
}

/**
 * Scripted Messages-compatible model endpoint.
 *
 * Slice 2 markers select write tools; slice 3 adds slow turns (`SLOW_TURN`),
 * long single replies (`HOLD`), child holds (`CHILD_HOLD`) and a real
 * `subagent` tool call (`SUBAGENT`) that spawns a slow child.
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
      const hasToolResult = lastMessage.includes('"tool_result"')

      if (hasToolResult) {
        response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' })
        const slow = marker(serialized, 'SLOW_TURN')
        if (slow !== undefined) {
          await finalMessage(response, body.model, 'SLICE3_SLOW_FINAL', slow)
          return
        }
        if (marker(serialized, 'SUBAGENT') !== undefined) {
          await finalMessage(response, body.model, 'SLICE3_PARENT_FINAL')
          return
        }
        await finalMessage(response, body.model, 'SLICE2_FINAL_DONE')
        return
      }

      const childHold = marker(lastMessage, 'CHILD_HOLD')
      if (childHold !== undefined) {
        response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' })
        await finalMessage(response, body.model, 'SLICE3_CHILD_FINAL', childHold)
        return
      }

      const subagent = marker(lastMessage, 'SUBAGENT')
      if (subagent !== undefined) {
        response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' })
        await toolCall(response, {
          model: body.model,
          name: 'subagent',
          input: {
            description: 'slice3 child work',
            prompt: `CHILD_HOLD ${subagent} run the slice3 child task`,
          },
        })
        return
      }

      const slowTurn = marker(lastMessage, 'SLOW_TURN')
      if (slowTurn !== undefined) {
        response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' })
        await toolCall(response, {
          model: body.model,
          name: 'write',
          holdMs: slowTurn,
          input: { file_path: 'slice3-slow.txt', content: `slow ${body.model}\n` },
        })
        return
      }

      const hold = /\bHOLD (\d+)/u.exec(lastMessage)
      if (hold !== null) {
        response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' })
        await finalMessage(response, body.model, 'SLICE3_HOLD_FINAL', Number(hold[1]))
        return
      }

      const markers = [...serialized.matchAll(/WRITE_(LOCAL|CROSS) ([^"\\\s]+)/gu)]
      const write = markers.at(-1)
      if (write?.[1] === 'LOCAL') {
        response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' })
        await toolCall(response, {
          model: body.model,
          name: 'write',
          input: { file_path: write[2], content: `written by ${body.model}\n` },
        })
        return
      }
      if (write?.[1] === 'CROSS') {
        response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' })
        await toolCall(response, {
          model: body.model,
          name: 'write',
          input: { file_path: write[2], content: 'cross-worktree write\n' },
        })
        return
      }
      response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' })
      await finalMessage(response, body.model, 'SLICE2_FINAL_DONE')
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
