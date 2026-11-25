/** Read the pinned Host's writable-root derivation and test path overlap. */

import { createRequire } from 'node:module'
import { isAbsolute, join, relative, sep } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const REPO_ROOT = fileURLToPath(new URL('..', import.meta.url))
const ANCHOR = join(REPO_ROOT, 'deepseek-harness/packages/fs/fs-sandbox/package.json')
const SENTINEL_ROOT = '/__dsh_liaison_no_workspace__'

let cached

async function loadSandbox() {
  if (cached !== undefined) return cached
  const resolved = createRequire(ANCHOR).resolve('@deepseek-ai/dsh-sandbox')
  cached = await import(pathToFileURL(resolved).href)
  return cached
}

/**
 * Platform temp roots that the Host's workspace-write policy makes globally
 * writable, derived by the same upstream `writableRoots` function the fs tool
 * and sandbox enforcement use. The sentinel workspace root contributes nothing
 * but is removed so only the platform grants remain.
 * @returns {Promise<string[]>} canonical temp roots.
 */
export async function hostTempWritableRoots() {
  const sandbox = await loadSandbox()
  return sandbox.writableRoots({
    mode: 'workspace-write',
    workspaceRoot: SENTINEL_ROOT,
    additionalWritableRoots: [],
  }).filter(root => root !== SENTINEL_ROOT)
}

/** True when either path contains the other after canonicalization. */
export function pathsOverlap(left, right) {
  return contains(left, right) || contains(right, left)
}

function contains(parent, child) {
  const rel = relative(parent, child)
  return rel === '' || (rel !== '..' && !rel.startsWith(`..${sep}`) && !isAbsolute(rel))
}
