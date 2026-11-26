import { realpath, stat } from 'node:fs/promises'
import { homedir } from 'node:os'
import { basename, dirname, join, resolve } from 'node:path'
import { resolveDshHome } from '@deepseek-ai/dsh-home-paths'
import { writableRoots } from '@deepseek-ai/dsh-sandbox'
import { pathsOverlap } from './delegation-registry.ts'

/** Stable failure kinds owned by delegation workspace validation. */
export const DELEGATION_ERROR_CODES = [
  'DELEGATION_SESSION_SUBSTITUTED',
  'DELEGATION_WORKSPACE_ATTACH_FAILED',
  'DELEGATION_SESSION_CONFLICT',
  'DELEGATION_TITLE_CONFLICT',
  'DELEGATION_MODEL_UNAVAILABLE',
  'DELEGATION_MODEL_MISMATCH',
  'DELEGATION_MODEL_EVIDENCE_MISSING',
  'DELEGATION_REQUEST_ID_UNCERTAIN',
  'DELEGATION_REQUEST_ID_EXPIRED',
  'WORKSPACE_INVALID_CWD',
  'WORKSPACE_UNISOLATABLE',
  'WORKSPACE_STATE_OVERLAP',
  'STATE_UNSAFE_TEMP_ROOT',
] as const

/** One known delegation failure kind. */
export type DelegationErrorCode = (typeof DELEGATION_ERROR_CODES)[number]

/** JSON-safe delegation failure details. */
export type DelegationErrorDetails = Readonly<Record<string, string | number | boolean | null>>

/** A typed delegation failure with a stable machine code. */
export class DelegationError extends Error {
  readonly code: DelegationErrorCode
  readonly details: DelegationErrorDetails

  /**
   * @param code - stable failure kind.
   * @param message - human-readable explanation.
   * @param details - optional flat, JSON-safe context.
   */
  constructor(code: DelegationErrorCode, message: string, details: DelegationErrorDetails = {}) {
    super(message)
    this.name = 'DelegationError'
    this.code = code
    this.details = details
  }
}

/**
 * Narrow an unknown failure to a delegation failure.
 * @param error - thrown value.
 * @returns whether the value is a DelegationError.
 */
export function isDelegationError(error: unknown): error is DelegationError {
  return error instanceof DelegationError
}

/** Host-owned directories that must never be delegated write roots. */
export interface WorkspaceGuardInput {
  /** Candidate working directory. */
  readonly cwd: string
  /** Host state directory from the verified Host record. */
  readonly stateDir: string
  /** Actual `DSH_HOME` reported by the Host. */
  readonly dshHome: string
  /** Durable delegation-ledger directory. */
  readonly ledgerDir: string
}

const SENTINEL = resolve('/dsh-liaison-writable-root-sentinel')

/**
 * Resolve an existing directory to its physical path.
 * @param path - directory path to canonicalize.
 * @returns the real absolute directory path.
 */
export async function canonicalDirectory(path: string): Promise<string> {
  const checked = resolve(path)
  let physical: string
  try {
    physical = await realpath(checked)
  } catch (error) {
    throw new DelegationError('WORKSPACE_INVALID_CWD', `workspace ${checked} cannot be resolved`, {
      cwd: checked,
      reason: errorMessage(error),
    })
  }
  let directory: boolean
  try {
    directory = (await stat(physical)).isDirectory()
  } catch (error) {
    throw new DelegationError('WORKSPACE_INVALID_CWD', `workspace ${checked} cannot be inspected`, {
      cwd: checked,
      reason: errorMessage(error),
    })
  }
  if (!directory) {
    throw new DelegationError('WORKSPACE_INVALID_CWD', `workspace ${checked} is not a directory`, {
      cwd: checked,
    })
  }
  return physical
}

/**
 * Validate one candidate delegation workspace against Host and platform writable roots.
 * @param input - candidate cwd plus verified Host and ledger state directories.
 * @returns the canonical workspace directory.
 */
export async function guardWorkspace(input: WorkspaceGuardInput): Promise<string> {
  const cwd = await canonicalDirectory(input.cwd)
  const tempRoots = writableRoots({
    mode: 'workspace-write',
    workspaceRoot: SENTINEL,
    additionalWritableRoots: [],
  }).filter(root => resolve(root) !== SENTINEL)

  const tempRoot = tempRoots.find(root => pathsOverlap(root, cwd))
  if (tempRoot !== undefined) {
    throw new DelegationError(
      'WORKSPACE_UNISOLATABLE',
      `workspace ${cwd} lies inside globally writable temporary root ${tempRoot}`,
      { cwd, tempRoot },
    )
  }

  const trusted = await Promise.all([
    canonicalizeTrustedPath(input.stateDir),
    canonicalizeTrustedPath(resolveDshHome(input.dshHome)),
    canonicalizeTrustedPath(input.ledgerDir),
  ])

  for (const stateDir of trusted) {
    const unsafeRoot = tempRoots.find(root => pathsOverlap(root, stateDir))
    if (unsafeRoot !== undefined) {
      throw new DelegationError(
        'STATE_UNSAFE_TEMP_ROOT',
        `trusted state directory ${stateDir} lies inside globally writable temporary root ${unsafeRoot}`,
        { stateDir, tempRoot: unsafeRoot },
      )
    }
  }

  const overlappingState = trusted.find(stateDir => pathsOverlap(stateDir, cwd))
  if (overlappingState !== undefined) {
    throw new DelegationError(
      'WORKSPACE_STATE_OVERLAP',
      `workspace ${cwd} overlaps trusted state directory ${overlappingState}`,
      { cwd, stateDir: overlappingState },
    )
  }

  const home = await canonicalizeTrustedPath(homedir())
  if (cwd === home) {
    throw new DelegationError(
      'WORKSPACE_STATE_OVERLAP',
      `workspace ${cwd} is the user home and contains trusted state`,
      { cwd, stateDir: home },
    )
  }

  return cwd
}

/**
 * Canonicalize a trusted path whose final components may not exist yet.
 * Resolves the nearest existing real ancestor and appends the missing suffix.
 * @param path - trusted state, ledger, or home path.
 * @returns the physical absolute path.
 */
async function canonicalizeTrustedPath(path: string): Promise<string> {
  const absolute = resolve(path)
  const missing: string[] = []
  let current = absolute
  while (true) {
    try {
      return join(await realpath(current), ...missing.reverse())
    } catch (error) {
      if ((error as NodeJS.ErrnoException | null)?.code !== 'ENOENT') {
        throw new DelegationError(
          'WORKSPACE_STATE_OVERLAP',
          `trusted path ${absolute} cannot be canonicalized`,
          { path: absolute, reason: errorMessage(error) },
        )
      }
      const parent = dirname(current)
      if (parent === current) {
        throw new DelegationError(
          'WORKSPACE_STATE_OVERLAP',
          `trusted path ${absolute} has no existing ancestor`,
          { path: absolute },
        )
      }
      missing.push(basename(current))
      current = parent
    }
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error && error.message !== '' ? error.message : 'unknown failure'
}
