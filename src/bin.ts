#!/usr/bin/env node
/** Executable entry for the stateless `dsh-liaison` CLI. */

import { runCli } from './cli.ts'

await runCli(process.argv.slice(2))
