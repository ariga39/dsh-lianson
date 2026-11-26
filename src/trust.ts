/** Trust-boundary checks for a delegation working directory. */

import { realpathSync, statSync } from 'node:fs'
import { dirname, isAbsolute, relative, resolve } from 'node:path'
import { writableRoots } from '@deepseek-ai/dsh-sandbox'
import type { SandboxExecutionPolicy } from '@deepseek-ai/dsh-sandbox'
import { LiaisonError } from './errors.ts'
import type { LiaisonPaths } from './paths.ts'

/** Directories a delegation may never use, plus the writable allow-list. */
export interface TrustPolicy {
  readonly dshHome: string
  readonly runtimeDir: string
  readonly ledgerDirectory: string
  /** Canonical writable roots from the deployment sandbox, or undefined without a policy. */
  readonly writableRoots: readonly string[] | undefined
}

/**
 * Build the trust policy from resolved paths and an optional sandbox policy.
 * `writableRoots()` is only consulted when the deployment exposes
 * `ctx.sandboxPolicy`; without it, protected roots still apply.
 * @param paths - resolved liaison paths.
 * @param policy - resolved sandbox execution policy, when the Host mounts one.
 * @returns the policy used by `create`.
 */
export function trustPolicyOf(paths: LiaisonPaths, policy: SandboxExecutionPolicy | undefined): TrustPolicy {
  return {
    dshHome: canonical(paths.dshHome),
    runtimeDir: canonical(paths.runtimeDir),
    ledgerDirectory: canonical(dirname(paths.ledgerPath)),
    writableRoots: policy === undefined ? undefined : writableRoots(policy).map(canonical),
  }
}

/**
 * Validate and canonicalize a delegation working directory.
 * @param cwd - caller-supplied directory.
 * @param policy - protected roots and optional writable allow-list.
 * @returns the physical directory path stored in the ledger.
 * @throws {LiaisonError} `invalid-cwd` or `unsafe-cwd`.
 */
export function assertTrustedCwd(cwd: string, policy: TrustPolicy): string {
  if (!isAbsolute(cwd) || cwd.includes('\0')) {
    throw new LiaisonError('invalid-cwd', 'cwd must be an absolute path without NUL bytes', { cwd })
  }
  let physical: string
  try {
    physical = realpathSync.native(resolve(cwd))
    if (!statSync(physical).isDirectory()) {
      throw new LiaisonError('invalid-cwd', `cwd is not a directory: ${cwd}`, { cwd })
    }
  } catch (error) {
    if (error instanceof LiaisonError) throw error
    throw new LiaisonError('invalid-cwd', `cwd cannot be read: ${cwd}`, { cwd })
  }
  for (const [root, label] of [
    [policy.dshHome, 'DSH_HOME'],
    [policy.runtimeDir, 'runtime directory'],
    [policy.ledgerDirectory, 'ledger directory'],
  ] as const) {
    if (isInside(root, physical)) {
      throw new LiaisonError('unsafe-cwd', `cwd must not be inside the ${label}`, { cwd: physical, root })
    }
  }
  if (policy.writableRoots !== undefined
    && !policy.writableRoots.some(root => isInside(root, physical))) {
    throw new LiaisonError('unsafe-cwd', 'cwd is outside every writable root configured for this deployment', {
      cwd: physical,
      writableRoots: [...policy.writableRoots],
    })
  }
  return physical
}

function canonical(path: string): string {
  try {
    return realpathSync.native(path)
  } catch {
    return resolve(path)
  }
}

function isInside(root: string, candidate: string): boolean {
  const rel = relative(root, candidate)
  return rel === '' || (!rel.startsWith('..') && !isAbsolute(rel))
}
