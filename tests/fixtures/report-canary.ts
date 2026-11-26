import { test } from 'node:test'
import { failureWithHostDiagnostics } from '../slice0/failure.ts'

const secret = process.env.DSH_CANARY_TOKEN
if (secret === undefined || secret === '') throw new Error('DSH_CANARY_TOKEN is required')

const deep = new Error(`deep cause ${secret}`)
const nested = new Error(`nested cause ?token=${secret}`, { cause: deep })
const outer = new Error(`page.goto: net::ERR_CONNECTION_REFUSED at http://127.0.0.1/?token=${secret}`)
outer.name = 'TimeoutError'
outer.cause = nested
const host = {
  async stop(): Promise<void> {},
  stderr(): string {
    return `reporter-final-stderr ?token=${secret}\n`
  },
}

await test('reporter canary throws a sanitized navigation failure', async () => {
  throw await failureWithHostDiagnostics({ error: outer, host, secret })
})
