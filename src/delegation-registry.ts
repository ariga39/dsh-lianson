import { homedir } from 'node:os'
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path'
import { acquireLock, type HeldLock } from './flock.ts'
import { ensurePrivateDirectory, readOptionalFile, writePrivateFile } from './fs-safe.ts'

/** Stable failure kinds owned by the delegation ledger. */
export const DELEGATION_REGISTRY_ERROR_CODES = [
  'DELEGATION_NOT_FOUND',
  'DELEGATION_ALREADY_EXISTS',
  'DELEGATION_REPLAY_CONFLICT',
  'DELEGATION_PROMPT_CONFLICT',
  'DELEGATION_TITLE_CONFLICT',
  'DELEGATION_LEDGER_READONLY',
  'DELEGATION_LEDGER_CLOSING',
  'DELEGATION_LEDGER_INVALID',
  'WORKSPACE_OVERLAP',
] as const

/** One known delegation-ledger failure kind. */
export type DelegationRegistryErrorCode = (typeof DELEGATION_REGISTRY_ERROR_CODES)[number]

/** JSON-safe ledger failure details. */
export type DelegationRegistryErrorDetails = Readonly<Record<string, string | number | boolean | null>>

/** A typed ledger failure with a stable machine code. */
export class DelegationRegistryError extends Error {
  readonly code: DelegationRegistryErrorCode
  readonly details: DelegationRegistryErrorDetails

  /**
   * @param code - stable failure kind.
   * @param message - human-readable explanation.
   * @param details - optional flat, JSON-safe context.
   */
  constructor(
    code: DelegationRegistryErrorCode,
    message: string,
    details: DelegationRegistryErrorDetails = {},
  ) {
    super(message)
    this.name = 'DelegationRegistryError'
    this.code = code
    this.details = details
  }
}

/**
 * Narrow an unknown failure to a ledger failure.
 * @param error - thrown value.
 * @returns whether the value is a DelegationRegistryError.
 */
export function isDelegationRegistryError(error: unknown): error is DelegationRegistryError {
  return error instanceof DelegationRegistryError
}

/** Absolute locations owned by one durable delegation ledger. */
export interface LedgerPaths {
  readonly ledgerDir: string
  readonly ledgerFile: string
  readonly ledgerLock: string
}

/** A provider/model pair selected for one delegation. */
export interface ModelTarget {
  readonly provider: string
  readonly model: string
  readonly reasoningEffort?: string
}

/** Lifecycle state persisted before and after a Host create. */
export type DelegationState = 'creating' | 'ready' | 'conflict' | 'attach-failed'

/** A write-ahead prompt receipt retained for the lifetime of the ledger. */
export interface PromptReceipt {
  readonly requestId: string
  readonly contentHash: string
  readonly beforeCursor: number
  readonly accepted: boolean
}

/** One durable delegation identity and its replay evidence. */
export interface DelegationRecord {
  readonly delegationId: string
  readonly workspaceId: string | null
  readonly sessionId: string
  readonly title: string
  readonly cwd: string
  readonly agentPreset?: string
  readonly expectedModel: ModelTarget
  readonly state: DelegationState
  readonly createFingerprint: string
  readonly createIpcId?: string
  readonly prompts?: Readonly<Record<string, PromptReceipt>>
  readonly verifiedModel?: ModelTarget
  readonly verifiedTurnSeq?: number
}

/** Input for the atomic write-ahead create transition. */
export interface CreateDelegationInput {
  readonly delegationId: string
  readonly sessionId: string
  readonly title: string
  readonly cwd: string
  readonly expectedModel: ModelTarget
  readonly createFingerprint: string
  readonly agentPreset?: string
  readonly createIpcId?: string
}

/** Result of an idempotent create transition. */
export interface CreateDelegationResult {
  readonly record: DelegationRecord
  readonly replayed: boolean
}

/** A non-creating state to which an admitted delegation may move. */
export type DelegationTransitionState = 'ready' | 'conflict' | 'attach-failed'

/** One atomic state transition. */
export interface DelegationTransition {
  readonly state: DelegationTransitionState
  readonly workspaceId?: string | null
}

/** Input for a write-ahead prompt receipt. */
export interface BeginPromptInput {
  readonly requestId: string
  readonly contentHash: string
  readonly beforeCursor: number
}

/** Result of an idempotent prompt admission. */
export interface PromptAdmission {
  readonly receipt: PromptReceipt
  readonly replayed: boolean
}

/** Monotonic model evidence from one completed turn. */
export interface VerifiedModelEvidence {
  readonly model: ModelTarget
  readonly turnSeq: number
}

/** Replaceable atomic writer; tests inject failures here. */
export type DelegationLedgerWriter = (path: string, content: string, mode?: number) => Promise<void>

/** Construction options for one ledger handle. */
export interface DelegationRegistryOptions {
  /** Environment used when `ledgerFile` is omitted. */
  readonly env?: NodeJS.ProcessEnv
  /** Explicit ledger path, bypassing `resolveLedgerPath`. */
  readonly ledgerFile?: string
  /** Bounded writer-lock contention wait, default 2000 ms. */
  readonly lockWaitMs?: number
  /** Atomic writer, default `writePrivateFile`. */
  readonly writeFile?: DelegationLedgerWriter
}

interface MutationPlan<T> {
  readonly value: T
  readonly changed: boolean
}

interface LedgerDocument {
  readonly version: 1
  readonly delegations: Record<string, DelegationRecord>
}

const LEDGER_VERSION = 1
const DEFAULT_LOCK_WAIT_MS = 2_000

/**
 * Resolve the durable ledger path without ever falling into ephemeral runtime state.
 * Precedence: `DSH_LIAISON_LEDGER_PATH`, `$XDG_STATE_HOME`, `$HOME/.local/state`.
 * @param env - environment mapping to read.
 * @returns an absolute ledger-file path.
 */
export function resolveLedgerPath(env: NodeJS.ProcessEnv = process.env): string {
  const override = env.DSH_LIAISON_LEDGER_PATH
  if (override !== undefined && override.trim() !== '') return resolve(override)
  const stateHome = env.XDG_STATE_HOME
  if (stateHome !== undefined && stateHome.trim() !== '') {
    return join(resolve(stateHome), 'dsh-liaison', 'delegations.json')
  }
  const home = env.HOME
  const base = home !== undefined && home.trim() !== '' ? resolve(home) : homedir()
  return join(base, '.local', 'state', 'dsh-liaison', 'delegations.json')
}

/**
 * Name the files inside one ledger directory.
 * @param ledgerFile - ledger JSON path.
 * @returns every durable ledger path.
 */
export function ledgerPaths(ledgerFile: string): LedgerPaths {
  const absolute = resolve(ledgerFile)
  const ledgerDir = dirname(absolute)
  return { ledgerDir, ledgerFile: absolute, ledgerLock: join(ledgerDir, 'delegations.lock') }
}

/**
 * Whether two paths are equal or one contains the other.
 * @param left - first path.
 * @param right - second path.
 * @returns true for exact equality or real containment.
 */
export function pathsOverlap(left: string, right: string): boolean {
  const first = resolve(left)
  const second = resolve(right)
  return within(first, second) || within(second, first)
}

function within(parent: string, child: string): boolean {
  const candidate = relative(parent, child)
  if (candidate === '') return true
  return !isAbsolute(candidate) && candidate !== '..' && !candidate.startsWith(`..${sep}`)
}

/**
 * Durable write-ahead ledger with a single flock writer and atomic publication.
 * Reads are lock-free until the caller explicitly becomes the writer.
 */
export class DelegationRegistry {
  private readonly paths: LedgerPaths
  private readonly writerWaitMs: number
  private readonly writeFile: DelegationLedgerWriter
  private records = new Map<string, DelegationRecord>()
  private writer: Promise<void> | undefined
  private writerLock: HeldLock | undefined
  private closing = false
  private closeTask: Promise<void> | undefined
  private writeTail: Promise<void> = Promise.resolve()

  /**
   * @param options - explicit path, environment, lock wait, or injected writer.
   */
  constructor(options: DelegationRegistryOptions = {}) {
    this.paths = ledgerPaths(options.ledgerFile ?? resolveLedgerPath(options.env))
    this.writerWaitMs = options.lockWaitMs ?? DEFAULT_LOCK_WAIT_MS
    this.writeFile = options.writeFile ?? writePrivateFile
  }

  /**
   * Acquire the single writer lock, re-reading the durable ledger first.
   * @param options - optional bounded contention wait for this call only.
   */
  async ensureWriter(options: { readonly waitMs?: number } = {}): Promise<void> {
    if (this.closing) throw closingError()
    await this.openWriter(options.waitMs ?? this.writerWaitMs, false)
  }

  /**
   * Fence new writes, drain admitted writes, then release the writer lock.
   * Safe to call more than once.
   */
  async close(): Promise<void> {
    this.closeTask ??= this.performClose()
    await this.closeTask
  }

  /**
   * Read one delegation by id.
   * @param delegationId - caller identity.
   * @returns a defensive copy of the durable record.
   */
  async get(delegationId: string): Promise<DelegationRecord> {
    await this.writeTail
    const records = await this.snapshot()
    const record = records.get(delegationId)
    if (record === undefined) {
      throw new DelegationRegistryError('DELEGATION_NOT_FOUND', `delegation ${delegationId} is not in the ledger`, {
        delegationId,
      })
    }
    return structuredClone(record)
  }

  /**
   * Read every durable delegation.
   * @returns defensive copies in ledger insertion order.
   */
  async list(): Promise<readonly DelegationRecord[]> {
    await this.writeTail
    const records = await this.snapshot()
    return [...records.values()].map(record => structuredClone(record))
  }

  /**
   * Atomically persist a write-ahead `creating` record or replay an existing one.
   * @param input - requested identity, workspace, title, and fingerprint.
   * @returns the durable record and whether this call replayed one.
   */
  async beginCreate(input: CreateDelegationInput): Promise<CreateDelegationResult> {
    const delegationId = requireText(input.delegationId, 'delegationId')
    const sessionId = requireText(input.sessionId, 'sessionId')
    const title = requireText(input.title, 'title')
    const createFingerprint = requireText(input.createFingerprint, 'createFingerprint')
    const cwd = resolve(input.cwd)
    const expectedModel = modelTarget(input.expectedModel)

    return await this.mutate<CreateDelegationResult>((draft): MutationPlan<CreateDelegationResult> => {
      const existing = draft.get(delegationId)
      if (existing !== undefined) {
        if (existing.createFingerprint !== createFingerprint) {
          throw new DelegationRegistryError(
            'DELEGATION_REPLAY_CONFLICT',
            `delegation ${delegationId} was created with different inputs`,
            { delegationId, state: existing.state },
          )
        }
        if (existing.state === 'conflict' || existing.state === 'attach-failed') {
          throw alreadyExists(delegationId, existing.state)
        }
        return { value: { record: structuredClone(existing), replayed: true }, changed: false }
      }

      for (const other of draft.values()) {
        if (other.state !== 'creating' && other.state !== 'ready') continue
        const otherCwd = resolve(other.cwd)
        if (otherCwd !== cwd && pathsOverlap(otherCwd, cwd)) {
          throw new DelegationRegistryError(
            'WORKSPACE_OVERLAP',
            `workspace ${cwd} overlaps active delegation ${other.delegationId}`,
            { delegationId, existingDelegationId: other.delegationId, cwd: otherCwd },
          )
        }
        if (otherCwd === cwd && other.title === title) {
          throw new DelegationRegistryError(
            'DELEGATION_TITLE_CONFLICT',
            `title ${title} already belongs to ${other.delegationId} in ${cwd}`,
            { delegationId, existingDelegationId: other.delegationId, title, cwd },
          )
        }
      }

      const record: DelegationRecord = {
        delegationId,
        workspaceId: null,
        sessionId,
        title,
        cwd,
        expectedModel,
        state: 'creating',
        createFingerprint,
        ...(input.agentPreset === undefined ? {} : { agentPreset: input.agentPreset }),
        ...(input.createIpcId === undefined ? {} : { createIpcId: input.createIpcId }),
      }
      draft.set(delegationId, record)
      return { value: { record: structuredClone(record), replayed: false }, changed: true }
    })
  }

  /**
   * Atomically move a creating delegation to ready, conflict, or attach-failed.
   * Terminal states are immutable; repeating the same terminal transition is a no-op.
   * @param delegationId - caller identity.
   * @param transition - target state and optional workspace binding.
   * @returns a defensive copy of the stored record.
   */
  async transition(delegationId: string, transition: DelegationTransition): Promise<DelegationRecord> {
    return await this.mutate((draft) => {
      const record = requireRecord(draft, delegationId)
      if (record.state === transition.state) {
        return { value: structuredClone(record), changed: false }
      }
      if (record.state !== 'creating') throw alreadyExists(delegationId, record.state)

      const next: DelegationRecord = {
        ...record,
        state: transition.state,
        ...(transition.workspaceId === undefined ? {} : { workspaceId: transition.workspaceId }),
      }
      draft.set(delegationId, next)
      return { value: structuredClone(next), changed: true }
    })
  }

  /**
   * Persist a prompt receipt before dispatch, or replay the original identity.
   * @param delegationId - owning delegation.
   * @param input - stable request id, content hash, and original cursor.
   * @returns the retained receipt and whether it was already accepted.
   */
  async beginPrompt(delegationId: string, input: BeginPromptInput): Promise<PromptAdmission> {
    const requestId = requireText(input.requestId, 'requestId')
    const contentHash = requireText(input.contentHash, 'contentHash')
    requireCursor(input.beforeCursor, 'beforeCursor')

    return await this.mutate((draft) => {
      const record = requireRecord(draft, delegationId)
      const existing = record.prompts?.[requestId]
      if (existing !== undefined) {
        if (existing.contentHash !== contentHash) {
          throw new DelegationRegistryError(
            'DELEGATION_PROMPT_CONFLICT',
            `request ${requestId} already carries different content`,
            { delegationId, requestId },
          )
        }
        return {
          value: { receipt: structuredClone(existing), replayed: existing.accepted },
          changed: false,
        }
      }
      if (record.state !== 'ready') throw alreadyExists(delegationId, record.state)

      const receipt: PromptReceipt = {
        requestId,
        contentHash,
        beforeCursor: input.beforeCursor,
        accepted: false,
      }
      const next: DelegationRecord = {
        ...record,
        prompts: { ...record.prompts, [requestId]: receipt },
      }
      draft.set(delegationId, next)
      return { value: { receipt: structuredClone(receipt), replayed: false }, changed: true }
    })
  }

  /**
   * Mark an admitted prompt receipt accepted after the Host acknowledged it.
   * @param delegationId - owning delegation.
   * @param requestId - stable request identity.
   * @returns the retained receipt.
   */
  async acceptPrompt(delegationId: string, requestId: string): Promise<PromptReceipt> {
    return await this.mutate((draft) => {
      const record = requireRecord(draft, delegationId)
      const receipt = requireReceipt(record, delegationId, requestId)
      if (receipt.accepted) return { value: structuredClone(receipt), changed: false }

      const nextReceipt: PromptReceipt = { ...receipt, accepted: true }
      const next: DelegationRecord = {
        ...record,
        prompts: { ...record.prompts, [requestId]: nextReceipt },
      }
      draft.set(delegationId, next)
      return { value: structuredClone(nextReceipt), changed: true }
    })
  }

  /**
   * Read one retained prompt receipt.
   * @param delegationId - owning delegation.
   * @param requestId - stable request identity.
   * @returns the receipt, or undefined when that request was never admitted.
   */
  async promptReceipt(delegationId: string, requestId: string): Promise<PromptReceipt | undefined> {
    const record = await this.get(delegationId)
    const receipt = record.prompts?.[requestId]
    return receipt === undefined ? undefined : structuredClone(receipt)
  }

  /**
   * Persist model evidence only when it is not older than the current evidence.
   * @param delegationId - owning delegation.
   * @param evidence - observed model and its turn sequence.
   * @returns the stored record, unchanged for stale evidence.
   */
  async recordVerifiedModel(delegationId: string, evidence: VerifiedModelEvidence): Promise<DelegationRecord> {
    requireCursor(evidence.turnSeq, 'turnSeq')
    const model = modelTarget(evidence.model)

    return await this.mutate((draft) => {
      const record = requireRecord(draft, delegationId)
      if (record.verifiedTurnSeq !== undefined && evidence.turnSeq < record.verifiedTurnSeq) {
        return { value: structuredClone(record), changed: false }
      }

      const next: DelegationRecord = {
        ...record,
        verifiedModel: model,
        verifiedTurnSeq: evidence.turnSeq,
      }
      draft.set(delegationId, next)
      return { value: structuredClone(next), changed: true }
    })
  }

  private async performClose(): Promise<void> {
    this.closing = true
    await this.writeTail
    const pending = this.writer
    if (pending !== undefined) await pending.catch(() => undefined)
    const lock = this.writerLock
    this.writerLock = undefined
    this.writer = undefined
    if (lock !== undefined) await lock.release()
  }

  private async openWriter(waitMs: number, admittedWrite: boolean): Promise<void> {
    if (this.writerLock !== undefined) return
    let pending = this.writer
    if (pending === undefined) {
      const acquisition = this.acquireWriter(waitMs, admittedWrite)
      this.writer = acquisition
      void acquisition.catch(() => {
        if (this.writer === acquisition) this.writer = undefined
      })
      pending = acquisition
    }
    await pending
  }

  private async acquireWriter(waitMs: number, admittedWrite: boolean): Promise<void> {
    await ensurePrivateDirectory(this.paths.ledgerDir).catch((error: unknown) => {
      throw new DelegationRegistryError('DELEGATION_LEDGER_READONLY', `cannot prepare ${this.paths.ledgerDir}`, {
        ledgerDir: this.paths.ledgerDir,
        reason: errorMessage(error),
      })
    })
    const lock = await acquireLock(this.paths.ledgerLock, { waitMs })
    if (lock === undefined) {
      throw new DelegationRegistryError(
        'DELEGATION_LEDGER_READONLY',
        `another writer holds ${this.paths.ledgerLock}`,
        { ledgerLock: this.paths.ledgerLock },
      )
    }

    try {
      if (this.closing && !admittedWrite) throw closingError()
      const loaded = await this.readLedger()
      if (this.closing && !admittedWrite) throw closingError()
      this.records = loaded
      this.writerLock = lock
    } catch (error) {
      await lock.release()
      throw error
    }
  }

  private async mutate<T>(operation: (draft: Map<string, DelegationRecord>) => MutationPlan<T>): Promise<T> {
    if (this.closing) throw closingError()
    const run = this.writeTail.then(async () => {
      await this.openWriter(this.writerWaitMs, true)
      const draft = new Map(this.records)
      const plan = operation(draft)
      if (plan.changed) {
        await this.persist(draft)
        this.records = draft
      }
      return plan.value
    })
    this.writeTail = run.then(() => undefined, () => undefined)
    return await run
  }

  private async persist(records: Map<string, DelegationRecord>): Promise<void> {
    const document: LedgerDocument = {
      version: LEDGER_VERSION,
      delegations: Object.fromEntries(records),
    }
    await this.writeFile(this.paths.ledgerFile, `${JSON.stringify(document, null, 2)}\n`, 0o600)
  }

  private async snapshot(): Promise<Map<string, DelegationRecord>> {
    if (this.writerLock !== undefined) return this.records
    return await this.readLedger()
  }

  private async readLedger(): Promise<Map<string, DelegationRecord>> {
    const text = await readOptionalFile(this.paths.ledgerFile)
    if (text === undefined) return new Map()
    return parseLedger(text, this.paths.ledgerFile)
  }
}

function parseLedger(text: string, ledgerFile: string): Map<string, DelegationRecord> {
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch (error) {
    throw invalidLedger(ledgerFile, `invalid JSON: ${errorMessage(error)}`)
  }
  if (!isObject(parsed)) throw invalidLedger(ledgerFile, 'document must be an object')
  if (parsed['version'] !== LEDGER_VERSION) throw invalidLedger(ledgerFile, 'unsupported ledger version')
  const delegations = parsed['delegations']
  if (!isObject(delegations)) throw invalidLedger(ledgerFile, 'delegations must be an object')

  const records = new Map<string, DelegationRecord>()
  for (const [delegationId, value] of Object.entries(delegations)) {
    records.set(delegationId, parseRecord(delegationId, value, ledgerFile))
  }
  return records
}

function parseRecord(delegationId: string, value: unknown, ledgerFile: string): DelegationRecord {
  if (!isObject(value)) throw invalidLedger(ledgerFile, `record ${delegationId} must be an object`)
  const storedId = value['delegationId']
  if (storedId !== delegationId) throw invalidLedger(ledgerFile, `record ${delegationId} has a mismatched id`)
  const workspaceId = value['workspaceId']
  if (workspaceId !== null && typeof workspaceId !== 'string') {
    throw invalidLedger(ledgerFile, `record ${delegationId} has an invalid workspaceId`)
  }
  const state = value['state']
  if (state !== 'creating' && state !== 'ready' && state !== 'conflict' && state !== 'attach-failed') {
    throw invalidLedger(ledgerFile, `record ${delegationId} has an invalid state`)
  }
  const prompts = parsePrompts(delegationId, value['prompts'], ledgerFile)
  const verifiedModel = value['verifiedModel']
  const verifiedTurnSeq = value['verifiedTurnSeq']
  if (verifiedTurnSeq !== undefined && !isCursor(verifiedTurnSeq)) {
    throw invalidLedger(ledgerFile, `record ${delegationId} has an invalid verifiedTurnSeq`)
  }
  const agentPreset = optionalText(value['agentPreset'], ledgerFile, `record ${delegationId} agentPreset`)
  const createIpcId = optionalText(value['createIpcId'], ledgerFile, `record ${delegationId} createIpcId`)

  return {
    delegationId,
    workspaceId,
    sessionId: requiredStoredText(value['sessionId'], ledgerFile, `record ${delegationId} sessionId`),
    title: requiredStoredText(value['title'], ledgerFile, `record ${delegationId} title`),
    cwd: requiredStoredText(value['cwd'], ledgerFile, `record ${delegationId} cwd`),
    expectedModel: parseModel(value['expectedModel'], ledgerFile, `record ${delegationId} expectedModel`),
    state,
    createFingerprint: requiredStoredText(
      value['createFingerprint'],
      ledgerFile,
      `record ${delegationId} createFingerprint`,
    ),
    ...(agentPreset === undefined ? {} : { agentPreset }),
    ...(createIpcId === undefined ? {} : { createIpcId }),
    ...(prompts === undefined ? {} : { prompts }),
    ...(verifiedModel === undefined
      ? {}
      : { verifiedModel: parseModel(verifiedModel, ledgerFile, `record ${delegationId} verifiedModel`) }),
    ...(verifiedTurnSeq === undefined ? {} : { verifiedTurnSeq }),
  }
}

function parsePrompts(
  delegationId: string,
  value: unknown,
  ledgerFile: string,
): Record<string, PromptReceipt> | undefined {
  if (value === undefined) return undefined
  if (!isObject(value)) throw invalidLedger(ledgerFile, `record ${delegationId} prompts must be an object`)
  const prompts = Object.create(null) as Record<string, PromptReceipt>
  for (const [requestId, receipt] of Object.entries(value)) {
    if (!isObject(receipt)) {
      throw invalidLedger(ledgerFile, `record ${delegationId} receipt ${requestId} must be an object`)
    }
    if (receipt['requestId'] !== requestId) {
      throw invalidLedger(ledgerFile, `record ${delegationId} receipt ${requestId} has a mismatched id`)
    }
    const beforeCursor = receipt['beforeCursor']
    if (!isCursor(beforeCursor)) {
      throw invalidLedger(ledgerFile, `record ${delegationId} receipt ${requestId} has an invalid cursor`)
    }
    const accepted = receipt['accepted']
    if (typeof accepted !== 'boolean') {
      throw invalidLedger(ledgerFile, `record ${delegationId} receipt ${requestId} has an invalid accepted flag`)
    }
    prompts[requestId] = {
      requestId,
      contentHash: requiredStoredText(
        receipt['contentHash'],
        ledgerFile,
        `record ${delegationId} receipt ${requestId} contentHash`,
      ),
      beforeCursor,
      accepted,
    }
  }
  return prompts
}

function parseModel(value: unknown, ledgerFile: string, field: string): ModelTarget {
  if (!isObject(value)) throw invalidLedger(ledgerFile, `${field} must be an object`)
  const provider = requiredStoredText(value['provider'], ledgerFile, `${field} provider`)
  const model = requiredStoredText(value['model'], ledgerFile, `${field} model`)
  const reasoningEffort = optionalText(value['reasoningEffort'], ledgerFile, `${field} reasoningEffort`)
  return reasoningEffort === undefined ? { provider, model } : { provider, model, reasoningEffort }
}

function requiredStoredText(value: unknown, ledgerFile: string, field: string): string {
  if (typeof value !== 'string' || value.trim() === '') throw invalidLedger(ledgerFile, `${field} must be text`)
  return value
}

function optionalText(value: unknown, ledgerFile: string, field: string): string | undefined {
  if (value === undefined) return undefined
  return requiredStoredText(value, ledgerFile, field)
}

function isCursor(value: unknown): value is number {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function invalidLedger(ledgerFile: string, reason: string): DelegationRegistryError {
  return new DelegationRegistryError('DELEGATION_LEDGER_INVALID', `invalid ledger ${ledgerFile}: ${reason}`, {
    ledgerFile,
    reason,
  })
}

function requireRecord(draft: Map<string, DelegationRecord>, delegationId: string): DelegationRecord {
  const record = draft.get(delegationId)
  if (record === undefined) {
    throw new DelegationRegistryError('DELEGATION_NOT_FOUND', `delegation ${delegationId} is not in the ledger`, {
      delegationId,
    })
  }
  return record
}

function requireReceipt(record: DelegationRecord, delegationId: string, requestId: string): PromptReceipt {
  const receipt = record.prompts?.[requestId]
  if (receipt === undefined) {
    throw new DelegationRegistryError('DELEGATION_NOT_FOUND', `request ${requestId} is not in the ledger`, {
      delegationId,
      requestId,
    })
  }
  return receipt
}

function requireText(value: string, field: string): string {
  if (value.trim() === '') {
    throw new DelegationRegistryError('DELEGATION_LEDGER_INVALID', `${field} must not be empty`, { field })
  }
  return value
}

function requireCursor(value: number, field: string): void {
  if (!isCursor(value)) {
    throw new DelegationRegistryError('DELEGATION_LEDGER_INVALID', `${field} must be a non-negative integer`, {
      field,
    })
  }
}

function modelTarget(model: ModelTarget): ModelTarget {
  const provider = requireText(model.provider, 'provider')
  const name = requireText(model.model, 'model')
  return model.reasoningEffort === undefined
    ? { provider, model: name }
    : { provider, model: name, reasoningEffort: requireText(model.reasoningEffort, 'reasoningEffort') }
}

function alreadyExists(delegationId: string, state: DelegationState): DelegationRegistryError {
  return new DelegationRegistryError(
    'DELEGATION_ALREADY_EXISTS',
    `delegation ${delegationId} is already ${state}`,
    { delegationId, state },
  )
}

function closingError(): DelegationRegistryError {
  return new DelegationRegistryError('DELEGATION_LEDGER_CLOSING', 'the delegation ledger is closing')
}

function errorMessage(error: unknown): string {
  return error instanceof Error && error.message !== '' ? error.message : 'unknown failure'
}
