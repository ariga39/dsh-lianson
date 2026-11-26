/** Trust-boundary checks for a delegation working directory. */

import { realpathSync, statSync } from 'node:fs'
import { dirname, isAbsolute, relative, resolve } from 'node:path'
import { writableRoots } from '@deepseek-ai/dsh-sandbox'
import type { SandboxExecutionPolicy } from '@deepseek-ai/dsh-sandbox'
import { LiaisonError } from './errors.ts'
import type { LiaisonPaths } from './paths.ts'

/** A directory a delegation Session must not own, plus shared writable areas. */
export interface TrustPolicy {
  /** Harness state; cwd must be neither inside nor an ancestor of any entry. */
  readonly protectedRoots: readonly string[]
  /** Global temp areas upstream adds to every `workspace-write` root set. */
  readonly globalWritableRoots: readonly string[]
}

/**
 * Neutral workspace root used only to ask upstream which additions are global.
 * It never exists, so it cannot collide with the platform temp areas.
 */
const NEUTRAL_WORKSPACE_ROOT = '/__dsh_liaison_neutral_workspace__'

/**
 * Build the trust policy from resolved paths and the deployment sandbox
 * policy. Upstream derives a Session's `workspace-write` boundary from that
 * Session's own cwd, so the deployment's agentless `resolve()` workspace root
 * is not an allow-list for new delegations. The only roots a new Session must
 * not own are protected Harness state and the shared temp areas that upstream
 * adds to every `workspace-write` policy.
 * @param paths - resolved liaison paths.
 * @param policy - deployment policy whose extra roots are shared by every
 * Session; such roots are not isolated enough to become a new workspace.
 * @returns the protected and globally shared roots.
 */
export function trustPolicyOf(paths: LiaisonPaths, policy?: SandboxExecutionPolicy): TrustPolicy {
  const neutral = { mode: 'workspace-write' as const, workspaceRoot: NEUTRAL_WORKSPACE_ROOT }
  const shared = (policy?.additionalWritableRoots ?? []).map(canonical)
  return {
    protectedRoots: [paths.dshHome, paths.runtimeDir, dirname(paths.ledgerPath)].map(canonical),
    globalWritableRoots: [...new Set([
      ...writableRoots(neutral)
        .filter(root => root !== NEUTRAL_WORKSPACE_ROOT)
        .map(canonical),
      ...shared,
    ])],
  }
}

/**
 * Validate and canonicalize a delegation working directory.
 * @param cwd - caller-supplied directory.
 * @param policy - protected and globally shared roots.
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
  // A Session rooted above Harness state could write that state, so ancestors
  // count too. A Session rooted above a shared temp area is harmless; only a
  // workspace inside the shared area is writable by unrelated processes.
  for (const root of policy.protectedRoots) {
    if (overlaps(root, physical)) {
      throw new LiaisonError('unsafe-cwd', 'cwd must not overlap protected Harness state', { cwd: physical, root })
    }
  }
  for (const root of policy.globalWritableRoots) {
    if (isInside(root, physical)) {
      throw new LiaisonError('unsafe-cwd', 'cwd must not be inside a globally shared writable root', { cwd: physical, root })
    }
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

/** Whether either path is inside the other, so ancestors and descendants both count. */
function overlaps(left: string, right: string): boolean {
  return isInside(left, right) || isInside(right, left)
}

function isInside(root: string, candidate: string): boolean {
  const rel = relative(root, candidate)
  return rel === '' || (!rel.startsWith('..') && !isAbsolute(rel))
}
