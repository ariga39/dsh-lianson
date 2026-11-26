import { setTimeout as delay } from 'node:timers/promises'

const mode = process.argv[2]

if (mode === 'fail') {
  process.stderr.write('failure-start token=supersecret\n')
  process.stderr.write('x'.repeat(200_000))
  process.stderr.write('\nfailure-end\n')
  process.exitCode = 1
} else {
  process.stdout.write('noise line\r\n')
  await delay(10)
  process.stdout.write('dsh web: http://127.0.0.1:9/?to')
  await delay(10)
  process.stdout.write('ken=framing-token\r\n')
  await delay(30_000)
}
