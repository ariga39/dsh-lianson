import { open } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { join } from 'node:path'
import { pathToFileURL } from 'node:url'

const anchor = join(import.meta.dirname, '../../deepseek-harness/native/system/packages/entry/package.json')
const resolved = createRequire(anchor).resolve('@deepseek-ai/node-addon-system/flock')
const { tryLockExclusive } = await import(pathToFileURL(resolved).href)

const handle = await open(process.argv[2], 'a+', 0o600)
await tryLockExclusive(handle.fd)
process.stdout.write('locked\n')
setInterval(() => {}, 1_000)
