import assert from 'node:assert/strict'
import { createServer } from 'node:http'

function deferred() {
  return Promise.withResolvers()
}

function sse(response, payload) {
  response.write(`event: ${payload.type}\ndata: ${JSON.stringify(payload)}\n\n`)
}

function toolCall(response, { id, name, input, model }) {
  sse(response, {
    type: 'message_start',
    message: { id: 'mock-message', type: 'message', role: 'assistant', model, content: [], usage: { input_tokens: 1, output_tokens: 0 } },
  })
  sse(response, { type: 'content_block_start', index: 0, content_block: { type: 'tool_use', id, name, input: {} } })
  sse(response, { type: 'content_block_delta', index: 0, delta: { type: 'input_json_delta', partial_json: JSON.stringify(input) } })
  sse(response, { type: 'content_block_stop', index: 0 })
  sse(response, { type: 'message_delta', delta: { stop_reason: 'tool_use', stop_sequence: null }, usage: { output_tokens: 1 } })
  sse(response, { type: 'message_stop' })
  response.end()
}

export async function startScriptedMessagesServer() {
  const gates = {
    step1Entered: deferred(),
    releaseStep1: deferred(),
    step2Entered: deferred(),
    releaseStep2: deferred(),
    step3Streaming: deferred(),
    releaseStep3: deferred(),
  }
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
      const attempt = requests.length
      if (attempt === 1) {
        gates.step1Entered.resolve()
        await gates.releaseStep1.promise
        response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' })
        toolCall(response, {
          id: 'mock-read', name: 'read', input: { file_path: 'fixture.txt' }, model: body.model,
        })
        return
      }
      if (attempt === 2) {
        assert.match(JSON.stringify(body), /SLICE0_FIXTURE_CONTENT/u, 'step 2 must carry the read result')
        gates.step2Entered.resolve()
        await gates.releaseStep2.promise
        response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' })
        toolCall(response, {
          id: 'mock-question',
          name: 'ask_user_question',
          input: {
            questions: [{
              id: 'q1',
              header: 'Confirm',
              question: 'Proceed with slice0?',
              options: [{ label: 'Yes (Recommended)' }, { label: 'No' }],
            }],
          },
          model: body.model,
        })
        return
      }
      if (attempt === 3) {
        assert.match(JSON.stringify(body), /Yes \(Recommended\)/u, 'step 3 must carry the external answer')
        response.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' })
        sse(response, {
          type: 'message_start',
          message: { id: 'mock-message', type: 'message', role: 'assistant', model: body.model, content: [], usage: { input_tokens: 1, output_tokens: 0 } },
        })
        sse(response, { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } })
        sse(response, { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'SLICE0_STREAM_PREFIX ' } })
        gates.step3Streaming.resolve()
        await gates.releaseStep3.promise
        sse(response, { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'SLICE0_FINAL_SENTINEL' } })
        sse(response, { type: 'content_block_stop', index: 0 })
        sse(response, { type: 'message_delta', delta: { stop_reason: 'end_turn', stop_sequence: null }, usage: { output_tokens: 2 } })
        sse(response, { type: 'message_stop' })
        response.end()
        return
      }
      errors.push(`unexpected request ${String(attempt)}`)
      response.writeHead(500).end('script exhausted')
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
    gates,
    requests,
    errors,
    close: () => new Promise((resolve, reject) => {
      server.closeAllConnections()
      server.close(error => error === undefined ? resolve() : reject(error))
    }),
  }
}
