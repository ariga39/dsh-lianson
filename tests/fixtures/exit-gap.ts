import { spawn } from 'node:child_process'

process.stdout.write('dsh web: http://127.0.0.1:9/?token=exit-gap-token\n')
const descendant = spawn(process.execPath, [
  '-e',
  "setTimeout(() => { process.stderr.write('EXIT_GAP_FINAL_STDERR\\n') }, 300)",
], {
  stdio: ['ignore', 'ignore', 'inherit'],
})
descendant.unref()
