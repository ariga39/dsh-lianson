/** Shared bootstrap readiness: resolvable inventory plus fresh-process import verification. */

import { spawnSync } from 'node:child_process'
import { existsSync, readdirSync, statSync } from 'node:fs'
import { createRequire } from 'node:module'
import { join } from 'node:path'
import { UPSTREAM_ENTRIES } from '../src/upstream-inventory.mjs'

const STATIC_ARTIFACTS = [
  'apps/cli/lib/bin.js',
  'apps/web/dist/index.html',
]

/** Platform packages documented by native/system: Linux/macOS only; Windows uses its own lock path. */
const NATIVE_SYSTEM_PLATFORMS = new Set(['linux-x64', 'linux-arm64', 'darwin-x64', 'darwin-arm64'])

export function requiresNativeSystem(platform = process.platform, arch = process.arch) {
  return NATIVE_SYSTEM_PLATFORMS.has(`${platform}-${arch}`)
}

function hasNamedFile(path, name) {
  if (!existsSync(path)) return false
  for (const entry of readdirSync(path)) {
    const child = join(path, entry)
    if (statSync(child).isDirectory()) {
      if (hasNamedFile(child, name)) return true
    } else if (entry === name) {
      return true
    }
  }
  return false
}

export function missingArtifacts(root, { platform = process.platform, arch = process.arch } = {}) {
  const missing = []
  for (const entry of UPSTREAM_ENTRIES) {
    try {
      const resolved = createRequire(join(root, entry.anchor)).resolve(entry.specifier)
      if (!existsSync(resolved)) missing.push(`${entry.key} ${entry.specifier} -> ${resolved}`)
    } catch (error) {
      missing.push(`${entry.key} ${entry.specifier}: ${error instanceof Error ? error.message : String(error)}`)
    }
  }
  for (const path of STATIC_ARTIFACTS) {
    if (!existsSync(join(root, path))) missing.push(path)
  }
  if (requiresNativeSystem(platform, arch)) {
    const platformPackage = join(root, 'native/system/packages', `${platform}-${arch}`)
    if (!hasNamedFile(platformPackage, 'system.node')) {
      missing.push(`native/system/packages/${platform}-${arch}/**/system.node`)
    }
  }
  return missing
}

export function verifyUpstreamImports(root) {
  const verifier = join(import.meta.dirname, 'verify-upstream-imports.mjs')
  const result = spawnSync(process.execPath, [verifier, root], { encoding: 'utf8' })
  return { ok: result.status === 0, stdout: result.stdout, stderr: result.stderr, status: result.status }
}

/** Final readiness = static host/Web/native outputs plus every public entry import. */
export function verifyUpstreamReady(root) {
  const missing = missingArtifacts(root)
  if (missing.length > 0) {
    return { ok: false, missing, stdout: '', stderr: `missing after build: ${missing.join(', ')}`, status: 1 }
  }
  return { ...verifyUpstreamImports(root), missing: [] }
}

/**
 * Ensure every static artifact and public entry imports. At most one rebuild is
 * performed, followed by one final combined static/import verification.
 */
export async function ensureUpstreamReady({ root, build, log = console.log }) {
  let built = false
  const missing = missingArtifacts(root)
  if (missing.length > 0) {
    log(`bootstrap: building missing upstream artifacts (${missing.join(', ')})`)
    await build()
    built = true
  }

  let check = verifyUpstreamReady(root)
  if (check.ok) return
  if (!built) {
    log('bootstrap: upstream readiness failed; rebuilding once and rechecking in a fresh process')
    await build()
    check = verifyUpstreamReady(root)
    if (check.ok) return
  }
  const detail = check.missing.length > 0 ? check.stderr : check.stderr || check.stdout
  throw new Error(`upstream artifacts/imports failed readiness after rebuild:\n${detail}`)
}
