/** `dsh-liaison` command surface: stateless JSON commands over the UDS socket. */

import { existsSync, readFileSync } from 'node:fs'
import { Command, CommanderError, Option } from 'commander'
import { sendCommand } from './client.ts'
import { asLiaisonError, LiaisonError, redact } from './errors.ts'
import { Ledger } from './ledger.ts'
import type { DelegationRecord } from './ledger.ts'
import { resolveLiaisonPaths } from './paths.ts'
import { ROUTES } from './protocol.ts'
import { PLUGIN_NAME, PLUGIN_VERSION } from './version.ts'

/** Injectable process boundary for tests. */
export interface CliDependencies {
  readonly env: NodeJS.ProcessEnv
  readonly send: (route: (typeof ROUTES)[keyof typeof ROUTES], body: unknown) => Promise<unknown>
  readonly stdout: (text: string) => void
  readonly stderr: (text: string) => void
  readonly setExitCode: (code: number) => void
}

/** Default dependencies backed by the real socket and process streams. */
export function defaultCliDependencies(env: NodeJS.ProcessEnv = process.env): CliDependencies {
  let socketPath: string | undefined
  return {
    env,
    send: (route, body) => {
      socketPath ??= resolveLiaisonPaths(env).socketPath
      return sendCommand(socketPath, route, body)
    },
    stdout: (text) => { process.stdout.write(text) },
    stderr: (text) => { process.stderr.write(text) },
    setExitCode: (code) => { process.exitCode = code },
  }
}

/**
 * Build the commander program.
 * @param deps - process, transport, and output seams.
 * @returns the unparsed program.
 */
export function createProgram(deps: CliDependencies): Command {
  const program = new Command()
  program
    .name(PLUGIN_NAME)
    .description('Delegate work to a DeepSeek Harness session and inspect the binding')
    .version(PLUGIN_VERSION)
    .showHelpAfterError()
    .exitOverride()
    .configureOutput({
      writeOut: text => { deps.stdout(text) },
      // Commander's own parse/usage text would precede the JSON failure
      // document; the CLI reports every parse error as JSON instead.
      writeErr: () => {},
    })

  program.command('status')
    .description('Report plugin health, DSH_HOME, origin, and loopback state')
    .action(async () => { await execute(deps, ROUTES.status, {}) })

  program.command('create')
    .description('Create or replay a delegation and its dsh session')
    .requiredOption('--delegation-id <id>', 'stable business key for this delegation')
    .requiredOption('--cwd <dir>', 'absolute working directory for the session')
    .option('--title <title>', 'session title; defaults to the delegation id')
    .option('--model <provider/model>', 'model route; defaults to the Host catalog default')
    .action(async (options: { delegationId: string; cwd: string; title?: string; model?: string }) => {
      await execute(deps, ROUTES.create, {
        delegationId: options.delegationId,
        cwd: options.cwd,
        ...(options.title === undefined ? {} : { title: options.title }),
        ...(options.model === undefined ? {} : { model: options.model }),
      })
    })

  program.command('get')
    .description('Return one stored delegation, falling back to the offline ledger')
    .requiredOption('--delegation-id <id>', 'delegation to read')
    .action(async (options: { delegationId: string }) => {
      await executeRead(deps, ROUTES.get, { delegationId: options.delegationId }, () => {
        const record = readOffline(deploymentPaths(deps)).find(options.delegationId)
        if (record === undefined) {
          throw new LiaisonError('not-found', `delegation ${options.delegationId} not found in the offline ledger`, {
            delegationId: options.delegationId,
          })
        }
        return { delegation: record, offline: true }
      })
    })

  program.command('list')
    .description('Return every stored delegation, falling back to the offline ledger')
    .action(async () => {
      await executeRead(deps, ROUTES.list, {}, () => ({
        delegations: readOffline(deploymentPaths(deps)).list(),
        offline: true,
      }))
    })

  program.command('prompt')
    .description('Submit one prompt with request-id replay semantics')
    .requiredOption('--delegation-id <id>', 'target delegation')
    .option('--request-id <id>', 'stable prompt key; generated when omitted')
    .addOption(new Option('--text <text>', 'prompt text').conflicts('file'))
    .addOption(new Option('--file <path>', 'read prompt text from a file').conflicts('text'))
    .action(async (options: { delegationId: string; requestId?: string; text?: string; file?: string }) => {
      const text = options.text ?? (options.file === undefined ? undefined : readPromptFile(options.file))
      if (text === undefined) throw new LiaisonError('usage', 'prompt requires --text or --file')
      if (options.text !== undefined && options.file !== undefined) {
        throw new LiaisonError('usage', 'use either --text or --file, not both')
      }
      await execute(deps, ROUTES.prompt, {
        delegationId: options.delegationId,
        ...(options.requestId === undefined ? {} : { requestId: options.requestId }),
        text,
      })
    })

  program.command('verify-model')
    .description('Compare the persisted request/header of a turn with the delegation model')
    .requiredOption('--delegation-id <id>', 'target delegation')
    .option('--request-id <id>', 'exact prompt to inspect; defaults to the latest turn')
    .action(async (options: { delegationId: string; requestId?: string }) => {
      await execute(deps, ROUTES.verifyModel, {
        delegationId: options.delegationId,
        ...(options.requestId === undefined ? {} : { requestId: options.requestId }),
      })
    })

  program.command('wait')
    .description('Wait across Host windows until input is required or the turn completes')
    .requiredOption('--delegation-id <id>', 'target delegation')
    .option('--after-seq <seq>', 'exclusive assistant-output cursor')
    .option('--timeout-ms <ms>', 'overall wait budget; defaults to 120000')
    .option('--slice-ms <ms>', 'one Host wait window; capped at 30000')
    .option('--max-messages <count>', 'assistant-output budget per window')
    .action(async (options: { delegationId: string; afterSeq?: string; timeoutMs?: string; sliceMs?: string; maxMessages?: string }) => {
      await execute(deps, ROUTES.wait, {
        delegationId: options.delegationId,
        ...(options.afterSeq === undefined ? {} : { afterSeq: integer(options.afterSeq, 'after-seq') }),
        ...(options.timeoutMs === undefined ? {} : { timeoutMs: integer(options.timeoutMs, 'timeout-ms') }),
        ...(options.sliceMs === undefined ? {} : { sliceTimeoutMs: integer(options.sliceMs, 'slice-ms') }),
        ...(options.maxMessages === undefined ? {} : { maxMessages: integer(options.maxMessages, 'max-messages') }),
      })
    })

  program.command('events')
    .description('Read a contiguous event range and expose the subagent catalog')
    .requiredOption('--delegation-id <id>', 'target delegation')
    .option('--after-seq <seq>', 'exclusive lower event cursor')
    .option('--through-seq <seq>', 'inclusive pinned event cut')
    .option('--page-size <count>', 'Host message budget per page')
    .action(async (options: { delegationId: string; afterSeq?: string; throughSeq?: string; pageSize?: string }) => {
      await execute(deps, ROUTES.events, {
        delegationId: options.delegationId,
        ...(options.afterSeq === undefined ? {} : { afterSeq: integer(options.afterSeq, 'after-seq') }),
        ...(options.throughSeq === undefined ? {} : { throughSeq: integer(options.throughSeq, 'through-seq') }),
        ...(options.pageSize === undefined ? {} : { pageSize: integer(options.pageSize, 'page-size') }),
      })
    })

  program.command('steer')
    .description('Add guidance to the running turn')
    .requiredOption('--delegation-id <id>', 'target delegation')
    .option('--request-id <id>', 'stable prompt key; generated when omitted')
    .addOption(new Option('--text <text>', 'guidance text').conflicts('file'))
    .addOption(new Option('--file <path>', 'read guidance text from a file').conflicts('text'))
    .action(async (options: { delegationId: string; requestId?: string; text?: string; file?: string }) => {
      await execute(deps, ROUTES.steer, {
        delegationId: options.delegationId,
        ...(options.requestId === undefined ? {} : { requestId: options.requestId }),
        text: promptText(options),
      })
    })

  program.command('queue')
    .description('Edit, remove, or steer one still-pending queue item')
    .requiredOption('--delegation-id <id>', 'target delegation')
    .requiredOption('--item-id <id>', 'pending queue item')
    .requiredOption('--action <kind>', 'edit | remove | steer')
    .option('--text <text>', 'replacement text for --action edit')
    .action(async (options: { delegationId: string; itemId: string; action: string; text?: string }) => {
      await execute(deps, ROUTES.queue, {
        delegationId: options.delegationId,
        itemId: options.itemId,
        action: queueAction(options.action, options.text),
      })
    })

  program.command('stop')
    .description('Clear queued input and cancel the active turn inside one time budget')
    .requiredOption('--delegation-id <id>', 'target delegation')
    .option('--timeout-ms <ms>', 'overall stop budget; defaults to 30000')
    .action(async (options: { delegationId: string; timeoutMs?: string }) => {
      await execute(deps, ROUTES.stop, {
        delegationId: options.delegationId,
        ...(options.timeoutMs === undefined ? {} : { timeoutMs: integer(options.timeoutMs, 'timeout-ms') }),
      })
    })

  program.command('rework')
    .description('Restore an archived Session and submit follow-up work to the original session id')
    .requiredOption('--delegation-id <id>', 'target delegation')
    .option('--request-id <id>', 'stable prompt key; generated when omitted')
    .addOption(new Option('--text <text>', 'rework text').conflicts('file'))
    .addOption(new Option('--file <path>', 'read rework text from a file').conflicts('text'))
    .action(async (options: { delegationId: string; requestId?: string; text?: string; file?: string }) => {
      await execute(deps, ROUTES.rework, {
        delegationId: options.delegationId,
        ...(options.requestId === undefined ? {} : { requestId: options.requestId }),
        text: promptText(options),
      })
    })

  const interaction = program.command('interaction')
    .description('Inspect and answer pending approvals and user questions')

  interaction.command('list')
    .description('List pending and continued interactions')
    .option('--delegation-id <id>', 'filter to one delegation')
    .action(async (options: { delegationId?: string }) => {
      await execute(deps, ROUTES.interactionList, options.delegationId === undefined ? {} : { delegationId: options.delegationId })
    })

  interaction.command('approve')
    .description('Approve one pending request')
    .requiredOption('--interaction-id <id>', 'pending interaction id')
    .action(async (options: { interactionId: string }) => {
      await execute(deps, ROUTES.interactionApprove, { interactionId: options.interactionId })
    })

  interaction.command('reject')
    .description('Reject one pending request')
    .requiredOption('--interaction-id <id>', 'pending interaction id')
    .action(async (options: { interactionId: string }) => {
      await execute(deps, ROUTES.interactionReject, { interactionId: options.interactionId })
    })

  interaction.command('answer')
    .description('Answer a pending or continued user question')
    .option('--interaction-id <id>', 'pending interaction id')
    .option('--delegation-id <id>', 'delegation for a continued question')
    .option('--call-id <id>', 'question tool call id')
    .requiredOption('--answers <json>', 'JSON answer array: [{"id":"q1","selected":["yes"]}]')
    .action(async (options: { interactionId?: string; delegationId?: string; callId?: string; answers: string }) => {
      await execute(deps, ROUTES.interactionAnswer, {
        ...(options.interactionId === undefined ? {} : { interactionId: options.interactionId }),
        ...(options.delegationId === undefined ? {} : { delegationId: options.delegationId }),
        ...(options.callId === undefined ? {} : { callId: options.callId }),
        answers: parseAnswers(options.answers),
      })
    })

  return program
}

async function execute(
  deps: CliDependencies,
  route: (typeof ROUTES)[keyof typeof ROUTES],
  body: unknown,
): Promise<void> {
  try {
    const value = await deps.send(route, body)
    deps.stdout(print(value))
  } catch (error) {
    fail(deps, error)
  }
}

async function executeRead(
  deps: CliDependencies,
  route: (typeof ROUTES)[keyof typeof ROUTES],
  body: unknown,
  offline: () => unknown,
): Promise<void> {
  try {
    deps.stdout(print(await deps.send(route, body)))
  } catch (error) {
    const failure = asLiaisonError(error)
    if (failure.code !== 'plugin-offline') {
      fail(deps, failure)
      return
    }
    try {
      deps.stdout(print(offline()))
    } catch (offlineError) {
      fail(deps, offlineError)
    }
  }
}

function fail(deps: CliDependencies, error: unknown): void {
  const failure = asLiaisonError(error)
  deps.stderr(`${print({
    ok: false,
    error: {
      code: failure.code,
      message: failure.message,
      ...(failure.details === undefined ? {} : { details: failure.details }),
    },
  })}\n`)
  deps.setExitCode(1)
}

function print(value: unknown): string {
  return `${JSON.stringify(redact(value), null, 2)}\n`
}

function deploymentPaths(deps: CliDependencies) {
  return resolveLiaisonPaths(deps.env)
}

function readOffline(paths: ReturnType<typeof resolveLiaisonPaths>): {
  find(delegationId: string): DelegationRecord | undefined
  list(): readonly DelegationRecord[]
} {
  if (!existsSync(paths.ledgerPath)) {
    return { find: () => undefined, list: () => [] }
  }
  const ledger = Ledger.open(paths.ledgerPath, { readOnly: true })
  return {
    find: (delegationId) => {
      try {
        return ledger.findDelegation(delegationId)
      } finally {
        ledger.close()
      }
    },
    list: () => {
      try {
        return ledger.listDelegations()
      } finally {
        ledger.close()
      }
    },
  }
}

function promptText(options: { readonly text?: string; readonly file?: string }): string {
  const text = options.text ?? (options.file === undefined ? undefined : readPromptFile(options.file))
  if (text === undefined) throw new LiaisonError('usage', 'command requires --text or --file')
  if (options.text !== undefined && options.file !== undefined) {
    throw new LiaisonError('usage', 'use either --text or --file, not both')
  }
  return text
}

function integer(value: string, name: string): number {
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed)) throw new LiaisonError('usage', `--${name} must be a safe integer`, { value })
  return parsed
}

function queueAction(kind: string, text: string | undefined): { kind: 'edit'; text: string } | { kind: 'remove' } | { kind: 'steer' } {
  switch (kind) {
    case 'edit':
      if (text === undefined) throw new LiaisonError('usage', '--action edit requires --text')
      return { kind: 'edit', text }
    case 'remove':
      return { kind: 'remove' }
    case 'steer':
      return { kind: 'steer' }
    default:
      throw new LiaisonError('usage', '--action must be edit, remove, or steer', { action: kind })
  }
}

function parseAnswers(value: string): { id: string; selected: string[]; custom?: string }[] {
  let parsed: unknown
  try {
    parsed = JSON.parse(value)
  } catch {
    throw new LiaisonError('usage', '--answers must be valid JSON')
  }
  const candidate = Array.isArray(parsed)
    ? parsed
    : parsed !== null && typeof parsed === 'object' && Array.isArray((parsed as { answers?: unknown }).answers)
      ? (parsed as { answers: unknown[] }).answers
      : undefined
  if (candidate === undefined) throw new LiaisonError('usage', '--answers must be a JSON array or {"answers": [...]}')
  return candidate.map((item) => {
    if (item === null || typeof item !== 'object') throw new LiaisonError('usage', 'each answer must be an object')
    const record = item as Record<string, unknown>
    if (typeof record.id !== 'string' || record.id.length === 0) {
      throw new LiaisonError('usage', 'each answer needs a non-empty id')
    }
    if (!Array.isArray(record.selected) || !record.selected.every(entry => typeof entry === 'string')) {
      throw new LiaisonError('usage', 'each answer needs a selected string array')
    }
    if (record.custom !== undefined && typeof record.custom !== 'string') {
      throw new LiaisonError('usage', 'answer custom must be a string')
    }
    return {
      id: record.id,
      selected: record.selected,
      ...(record.custom === undefined ? {} : { custom: record.custom }),
    }
  })
}

function readPromptFile(path: string): string {
  try {
    return readFileSync(path, 'utf8')
  } catch (error) {
    throw new LiaisonError('usage', `cannot read prompt file ${path}: ${error instanceof Error ? error.message : String(error)}`)
  }
}

/**
 * Parse argv and run one command, reporting failures as a redacted JSON
 * document on stderr.
 * @param argv - command arguments after the executable.
 * @param deps - process, transport, and output seams.
 * @returns when the command or its failure report finishes.
 */
export async function runCli(argv: readonly string[], deps: CliDependencies = defaultCliDependencies()): Promise<void> {
  try {
    await createProgram(deps).parseAsync([...argv], { from: 'user' })
  } catch (error) {
    if (error instanceof CommanderError) {
      if (error.code === 'commander.helpDisplayed' || error.code === 'commander.version') return
      fail(deps, new LiaisonError('usage', error.message))
      return
    }
    fail(deps, error)
  }
}
