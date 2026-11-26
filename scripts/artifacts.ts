import { existsSync, readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { dirname, join, resolve } from 'node:path'

/** One file the prepared upstream checkout must provide before reuse. */
export interface ArtifactRequirement {
  readonly label: string
  readonly path: string
}

const REQUIRED_SPECIFIERS = [
  '@deepseek-ai/cordis',
  '@deepseek-ai/dsh-api-gateway/client',
  '@deepseek-ai/dsh-api-remotes/types',
  '@deepseek-ai/dsh-api-session-controller/remote',
  '@deepseek-ai/dsh-api-session-controller/types',
  '@deepseek-ai/dsh-api-workspace-controller/remote',
  '@deepseek-ai/dsh-brand',
  '@deepseek-ai/dsh-client-connection/client',
  '@deepseek-ai/dsh-commands/remote',
  '@deepseek-ai/dsh-llm',
  '@deepseek-ai/dsh-permission-presets/remote',
  '@deepseek-ai/dsh-session/types',
  '@deepseek-ai/dsh-typert-protocol',
  '@deepseek-ai/dsh-typert-registry',
  '@deepseek-ai/dsh-user-questions/remote',
  '@deepseek-ai/dsh-user-questions/types',
] as const

/**
 * List the upstream built files this project resolves.
 * @param upstreamRoot - prepared DeepSeek Harness checkout.
 * @returns every required runtime module, declaration, and entry asset.
 */
export function upstreamArtifactRequirements(upstreamRoot: string): ArtifactRequirement[] {
  const root = resolve(upstreamRoot)
  const requirements: ArtifactRequirement[] = [
    { label: 'CLI bin', path: join(root, 'apps/cli/lib/bin.js') },
    { label: 'Web index', path: join(root, 'apps/web/dist/index.html') },
    {
      label: 'native addon',
      path: join(root, `native/system/packages/linux-${process.arch}/bin/glibc/system.node`),
    },
  ]
  const require = createRequire(join(root, 'apps/cli/package.json'))
  for (const specifier of REQUIRED_SPECIFIERS) {
    try {
      const jsPath = require.resolve(specifier)
      requirements.push({ label: `${specifier} JS`, path: jsPath })
      const packageJsonPath = findPackageJson(jsPath)
      const packageJson = parseJsonRecord(readFileSync(packageJsonPath, 'utf8'), packageJsonPath)
      const packageName = stringField(packageJson, 'name')
      if (packageName === undefined) throw new Error(`${packageJsonPath} has no package name`)
      const subpath = specifier === packageName ? '.' : `.${specifier.slice(packageName.length)}`
      const entry = recordOf(packageJson['exports'])?.[subpath]
      const types = recordOf(entry)?.types ?? stringField(packageJson, 'types')
      if (typeof types !== 'string') throw new Error(`${specifier} declares no type declaration entry`)
      requirements.push({ label: `${specifier} types`, path: resolve(dirname(packageJsonPath), types) })
    } catch {
      requirements.push({ label: `${specifier} (unresolved)`, path: specifier })
    }
  }
  return requirements
}

/**
 * List missing upstream files, including assets referenced by the Web entry.
 * @param upstreamRoot - prepared DeepSeek Harness checkout.
 * @returns human-readable missing requirements; empty when the checkout is ready.
 */
export function missingUpstreamArtifacts(upstreamRoot: string): string[] {
  const root = resolve(upstreamRoot)
  const missing = upstreamArtifactRequirements(root)
    .filter(requirement => !existsSync(requirement.path))
    .map(requirement => `${requirement.label}: ${requirement.path}`)
  const indexPath = join(root, 'apps/web/dist/index.html')
  if (existsSync(indexPath)) missing.push(...missingWebAssets(indexPath))
  return missing
}

function missingWebAssets(indexPath: string): string[] {
  const directory = dirname(indexPath)
  const html = readFileSync(indexPath, 'utf8')
  const missing: string[] = []
  for (const match of html.matchAll(/(?:src|href)="([^"]+)"/gu)) {
    const reference = match[1]
    if (reference === undefined || reference.startsWith('//') || reference.startsWith('#')) continue
    if (/^[a-z][a-z0-9+.-]*:/iu.test(reference)) continue
    const target = reference.startsWith('/') ? join(directory, reference.slice(1)) : resolve(directory, reference)
    if (!existsSync(target)) missing.push(`Web asset ${reference}: ${target}`)
  }
  return missing
}

function findPackageJson(file: string): string {
  let directory = dirname(file)
  while (true) {
    const candidate = join(directory, 'package.json')
    if (existsSync(candidate)) return candidate
    const parent = dirname(directory)
    if (parent === directory) throw new Error(`no package.json found above ${file}`)
    directory = parent
  }
}

function parseJsonRecord(text: string, source: string): Record<string, unknown> {
  const parsed: unknown = JSON.parse(text)
  const record = recordOf(parsed)
  if (record === undefined) throw new Error(`${source} is not a JSON object`)
  return record
}

function stringField(value: Record<string, unknown>, key: string): string | undefined {
  const field = value[key]
  return typeof field === 'string' ? field : undefined
}

function recordOf(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined
}
