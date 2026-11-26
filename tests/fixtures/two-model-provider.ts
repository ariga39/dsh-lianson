import { join } from 'node:path'
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
/** Sentinel the scripted write tool attempts in the sibling checkout. */
export const TWO_MODEL_BLOCKED_FILE = 'cross-checkout-sentinel.txt'
/** Final answer prefix. */
export const TWO_MODEL_DONE = 'TWO_MODEL_DONE'

/** Paths the scripted cross-checkout write attempts target. */
export interface TwoModelProviderConfig {
  readonly worktreeA: string
  readonly worktreeB: string
}

export const name = 'slice2-two-model-provider'
export const inject = ['llm']

/** Mount one keyless provider that advertises two distinct models. */
export function apply(ctx: Context, config: TwoModelProviderConfig): void {
  ctx.llm.registerAdapter([TWO_MODEL_PROVIDER], new TwoModelAdapter(config))
}

class TwoModelAdapter extends LlmAdapter {
  private readonly config: TwoModelProviderConfig
  private callSequence = 0

  constructor(config: TwoModelProviderConfig) {
    super()
    this.config = config
  }

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

    const session = String(options.sessionId ?? 'unknown')
    const ownFile = JSON.stringify({
      file_path: TWO_MODEL_WRITE_FILE,
      content: `${options.model}:${session}`,
    })
    const siblingRoot = options.model === TWO_MODEL_A ? this.config.worktreeB : this.config.worktreeA
    const blockedFile = JSON.stringify({
      file_path: join(siblingRoot, TWO_MODEL_BLOCKED_FILE),
      content: `cross-checkout:${options.model}:${session}`,
    })
    yield* this.call(0, ownFile)
    yield* this.call(1, blockedFile)
    yield { type: 'usage', usage: { inputTokens: 12, outputTokens: 6 } }
    yield { type: 'finish', reason: { kind: 'tool-calls' } }
  }

  private *call(index: number, argumentsJson: string): Iterable<StreamChunk> {
    const callId = brandString<ToolCallId>(
      `two-model-write-${String(index)}-${String(this.callSequence += 1)}`,
    )
    yield { type: 'block-start', index, blockType: 'tool-call' }
    yield {
      type: 'tool-call-delta',
      index,
      id: callId,
      name: 'write',
      argumentsDelta: argumentsJson,
    }
    yield {
      type: 'block-end',
      index,
      block: { type: 'tool-call', id: callId, name: 'write', arguments: argumentsJson },
    }
  }
}
