import { brandString } from '@deepseek-ai/dsh-brand'
import type { Context } from '@deepseek-ai/cordis'
import {
  LlmAdapter,
  type GenerateOptions,
  type LlmModelInfo,
  type LlmResolvedModelInfo,
  type StreamChunk,
  type ToolCallId,
} from '@deepseek-ai/dsh-llm'

/** Provider route owned by this fixture. */
export const TWO_MODEL_PROVIDER = 'slice2-two-model'
/** First advertised model. */
export const TWO_MODEL_A = 'model-a'
/** Second advertised model. */
export const TWO_MODEL_B = 'model-b'
/** Relative file the scripted write tool creates in the Session cwd. */
export const TWO_MODEL_WRITE_FILE = 'isolation.txt'
/** Final answer prefix. */
export const TWO_MODEL_DONE = 'TWO_MODEL_DONE'

export const name = 'slice2-two-model-provider'
export const inject = ['llm']

/** Mount one keyless provider that advertises two distinct models. */
export function apply(ctx: Context): void {
  ctx.llm.registerAdapter([TWO_MODEL_PROVIDER], new TwoModelAdapter())
}

class TwoModelAdapter extends LlmAdapter {
  private callSequence = 0

  override listModels(provider: string): Promise<readonly LlmModelInfo[]> {
    return Promise.resolve([
      { provider, id: TWO_MODEL_A, name: 'Two Model A', contextWindow: 128_000 },
      { provider, id: TWO_MODEL_B, name: 'Two Model B', contextWindow: 128_000 },
    ])
  }

  override resolveModel(provider: string, model: string): Promise<LlmResolvedModelInfo> {
    return Promise.resolve({ provider, id: model, name: `Two Model ${model}`, contextWindow: 128_000 })
  }

  async *stream(options: GenerateOptions): AsyncIterable<StreamChunk> {
    const last = options.messages.at(-1)
    if (last?.role === 'tool') {
      yield { type: 'block-start', index: 0, blockType: 'text' }
      yield {
        type: 'text-delta',
        index: 0,
        text: `${TWO_MODEL_DONE} ${options.model}`,
      }
      yield {
        type: 'block-end',
        index: 0,
        block: { type: 'text', text: `${TWO_MODEL_DONE} ${options.model}` },
      }
      yield { type: 'usage', usage: { inputTokens: 7, outputTokens: 4 } }
      yield { type: 'finish', reason: { kind: 'stop' } }
      return
    }

    const callId = brandString<ToolCallId>(
      `two-model-write-${String(options.sessionId ?? 'session')}-${String(this.callSequence += 1)}`,
    )
    const argumentsJson = JSON.stringify({
      file_path: TWO_MODEL_WRITE_FILE,
      content: `${options.model}:${String(options.sessionId ?? 'unknown')}`,
    })
    yield { type: 'block-start', index: 0, blockType: 'tool-call' }
    yield {
      type: 'tool-call-delta',
      index: 0,
      id: callId,
      name: 'write',
      argumentsDelta: argumentsJson,
    }
    yield {
      type: 'block-end',
      index: 0,
      block: { type: 'tool-call', id: callId, name: 'write', arguments: argumentsJson },
    }
    yield { type: 'usage', usage: { inputTokens: 9, outputTokens: 3 } }
    yield { type: 'finish', reason: { kind: 'tool-calls' } }
  }
}
