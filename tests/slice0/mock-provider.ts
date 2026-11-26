import { access, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import type { Context } from '@deepseek-ai/cordis'
import { brandString } from '@deepseek-ai/dsh-brand'
import {
  LlmAdapter,
  type GenerateOptions,
  type LlmModelInfo,
  type LlmResolvedModelInfo,
  type StreamChunk,
  type ToolCallId,
} from '@deepseek-ai/dsh-llm'
import {
  MOCK_MODEL,
  MOCK_PROVIDER,
  QUESTION_CALL_ID,
  QUESTION_ID,
  QUESTION_OPTION,
  READ_CALL_ID,
} from './constants.ts'

/** Configuration passed through the Host `--patch` row. */
export interface MockProviderConfig {
  /** Directory shared with the test for the gated stream handshakes. */
  readonly controlDir: string
  /** Absolute file the scripted read tool must return. */
  readonly readPath: string
  /** Unique final-answer marker. */
  readonly nonce: string
}

const READ_TOOL_CALL = brandString<ToolCallId>(READ_CALL_ID)
const QUESTION_TOOL_CALL = brandString<ToolCallId>(QUESTION_CALL_ID)
const CONTROL_TIMEOUT_MS = 30_000

export const name = 'slice0-mock-provider'
export const inject = ['llm']

/** Mount the keyless scripted provider. */
export function apply(ctx: Context, config: MockProviderConfig): void {
  ctx.llm.registerAdapter([MOCK_PROVIDER], new ScriptedAdapter(config))
}

class ScriptedAdapter extends LlmAdapter {
  private readonly config: MockProviderConfig

  constructor(config: MockProviderConfig) {
    super()
    this.config = config
  }

  override listModels(provider: string): Promise<readonly LlmModelInfo[]> {
    return Promise.resolve([{ provider, id: MOCK_MODEL, name: 'Slice 0 Scripted', contextWindow: 128_000 }])
  }

  override resolveModel(provider: string, model: string): Promise<LlmResolvedModelInfo> {
    return Promise.resolve({ provider, id: model, name: 'Slice 0 Scripted', contextWindow: 128_000 })
  }

  async *stream(options: GenerateOptions): AsyncIterable<StreamChunk> {
    const last = options.messages.at(-1)
    if (last?.role === 'tool' && last.toolCallId === READ_TOOL_CALL) {
      yield* this.askQuestion()
      return
    }
    if (last?.role === 'tool' && last.toolCallId === QUESTION_TOOL_CALL) {
      yield* this.finishAnswer(options.signal)
      return
    }
    yield* this.readSentinel(options.signal)
  }

  private async *readSentinel(signal: AbortSignal | undefined): AsyncIterable<StreamChunk> {
    await this.writeControl('step1-started')
    await this.waitForControl('step1-release', signal)
    const argumentsJson = JSON.stringify({ file_path: this.config.readPath })
    yield { type: 'block-start', index: 0, blockType: 'tool-call' }
    yield {
      type: 'tool-call-delta',
      index: 0,
      id: READ_TOOL_CALL,
      name: 'read',
      argumentsDelta: argumentsJson,
    }
    yield {
      type: 'block-end',
      index: 0,
      block: { type: 'tool-call', id: READ_TOOL_CALL, name: 'read', arguments: argumentsJson },
    }
    yield { type: 'usage', usage: { inputTokens: 11, outputTokens: 3 } }
    yield { type: 'finish', reason: { kind: 'tool-calls' } }
  }

  private async *askQuestion(): AsyncIterable<StreamChunk> {
    const argumentsJson = JSON.stringify({
      questions: [{
        id: QUESTION_ID,
        question: 'Proceed with Slice 0 after the external operator answered?',
        header: 'Slice 0',
        options: [
          { label: QUESTION_OPTION, description: 'Release the scripted final step.' },
          { label: 'Stop', description: 'Keep the question pending.' },
        ],
      }],
    })
    yield { type: 'block-start', index: 0, blockType: 'tool-call' }
    yield {
      type: 'tool-call-delta',
      index: 0,
      id: QUESTION_TOOL_CALL,
      name: 'ask_user_question',
      argumentsDelta: argumentsJson,
    }
    yield {
      type: 'block-end',
      index: 0,
      block: { type: 'tool-call', id: QUESTION_TOOL_CALL, name: 'ask_user_question', arguments: argumentsJson },
    }
    yield { type: 'usage', usage: { inputTokens: 13, outputTokens: 4 } }
    yield { type: 'finish', reason: { kind: 'tool-calls' } }
  }

  private async *finishAnswer(signal: AbortSignal | undefined): AsyncIterable<StreamChunk> {
    const partial = `SLICE0_STREAM_PARTIAL ${this.config.nonce}`
    const final = ` SLICE0_FINAL_OK ${this.config.nonce}`
    yield { type: 'block-start', index: 0, blockType: 'text' }
    yield { type: 'text-delta', index: 0, text: partial }
    await this.writeControl('step3-started')
    await this.waitForControl('step3-release', signal)
    yield { type: 'text-delta', index: 0, text: final }
    yield { type: 'block-end', index: 0, block: { type: 'text', text: `${partial}${final}` } }
    yield { type: 'usage', usage: { inputTokens: 17, outputTokens: 9 } }
    yield { type: 'finish', reason: { kind: 'stop' } }
  }

  private async writeControl(file: string): Promise<void> {
    await writeFile(join(this.config.controlDir, file), 'ready\n')
  }

  private async waitForControl(file: string, signal: AbortSignal | undefined): Promise<void> {
    const path = join(this.config.controlDir, file)
    const deadline = Date.now() + CONTROL_TIMEOUT_MS
    while (true) {
      if (signal?.aborted === true) throw signal.reason
      try {
        await access(path)
        return
      } catch {
        if (Date.now() >= deadline) throw new Error(`slice 0 provider timed out waiting for ${file}`)
        await pause(20, signal)
      }
    }
  }
}

async function pause(ms: number, signal: AbortSignal | undefined): Promise<void> {
  if (signal === undefined) {
    await delay(ms)
    return
  }
  await delay(ms, undefined, { signal })
}
