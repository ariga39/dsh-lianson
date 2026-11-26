import {
  DELEGATION_REGISTRY_ERROR_CODES,
  isDelegationRegistryError,
} from './delegation-registry.ts'
import {
  DELEGATION_ERROR_CODES,
  isDelegationError,
} from './workspace-guard.ts'

/** Closed error vocabulary shared by the daemon, CLI, and lifecycle modules. */
export const LIAISON_ERROR_CODES = [
  'INVALID_ARGUMENT',
  'INTERNAL',
  'HOST_IDENTITY_UNSUPPORTED',
  'HOST_IDENTITY_AMBIGUOUS',
  'HOST_IDENTITY_UNVERIFIED',
  'HOST_RECORD_INVALID',
  'HOST_LAUNCH_INCOMPLETE',
  'HOST_AUTH_UNAVAILABLE',
  'HOST_START_FAILED',
  'HOST_START_TIMEOUT',
  'HOST_NOT_RUNNING',
  'HOST_UNREACHABLE',
  'HOST_STOP_FAILED',
  'DAEMON_STOPPING',
  'IPC_UNAVAILABLE',
  'IPC_PROTOCOL',
  'IPC_TIMEOUT',
  'OPERATOR_UNAVAILABLE',
  ...DELEGATION_REGISTRY_ERROR_CODES,
  ...DELEGATION_ERROR_CODES,
] as const

/** One known failure kind. */
export type LiaisonErrorCode = (typeof LIAISON_ERROR_CODES)[number]

/** JSON-safe details that never carry credentials. */
export type LiaisonErrorDetails = Readonly<Record<string, string | number | boolean | null>>

/** A typed failure with a stable machine code and optional safe details. */
export class LiaisonError extends Error {
  readonly code: LiaisonErrorCode
  readonly details: LiaisonErrorDetails | undefined

  /**
   * @param code - stable failure kind.
   * @param message - redacted human-readable explanation.
   * @param options - optional cause and safe structured details.
   */
  constructor(
    code: LiaisonErrorCode,
    message: string,
    options: { readonly cause?: unknown; readonly details?: LiaisonErrorDetails } = {},
  ) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause })
    this.name = 'LiaisonError'
    this.code = code
    this.details = options.details
  }
}

/** Wire form sent over the local IPC socket. */
export interface WireError {
  readonly code: LiaisonErrorCode
  readonly message: string
  readonly details?: LiaisonErrorDetails
}

/**
 * Narrow an unknown failure to a LiaisonError.
 * @param error - thrown value.
 * @returns whether the value is a LiaisonError.
 */
export function isLiaisonError(error: unknown): error is LiaisonError {
  return error instanceof LiaisonError
}

/**
 * Preserve a LiaisonError, otherwise wrap the failure under a fallback code.
 * @param error - thrown value.
 * @param fallback - code for untyped failures.
 * @param message - optional fallback message.
 * @returns a typed failure.
 */
export function asLiaisonError(
  error: unknown,
  fallback: LiaisonErrorCode = 'INTERNAL',
  message = 'unexpected failure',
): LiaisonError {
  if (isLiaisonError(error)) return error
  if (isDelegationRegistryError(error) || isDelegationError(error)) {
    return new LiaisonError(error.code, error.message, { cause: error, details: error.details })
  }
  const detail = error instanceof Error && error.message !== '' ? error.message : message
  return new LiaisonError(fallback, detail, { cause: error })
}

/**
 * Build the wire-safe error envelope.
 * @param error - thrown value.
 * @returns code, message, and optional details.
 */
export function wireErrorOf(error: unknown): WireError {
  const failure = asLiaisonError(error)
  return failure.details === undefined
    ? { code: failure.code, message: failure.message }
    : { code: failure.code, message: failure.message, details: failure.details }
}
