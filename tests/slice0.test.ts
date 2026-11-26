import { strict as assert } from 'node:assert'
import { test } from 'node:test'
import { runSlice0 } from './slice0/smoke.ts'

await test('slice 0: keyless external operator against a real dsh web instance', { timeout: 300_000 }, async () => {
  const report = await runSlice0()
  assert.equal(report.duplicateInFlightAccepted, true)
  assert.equal(report.duplicateAfterStepAccepted, true)
  assert.equal(report.userMessages, 1)
  assert.equal(report.contiguousEvents, true)
  assert.equal(report.turnEndedCompleted, true)
  assert.equal(report.pageErrors, 0)
  assert.equal(report.writerHeldErrors, 0)
  assert.equal(report.modelProvider, 'slice0-mock')
  assert.equal(report.modelId, 'slice0-scripted')
  assert.ok(report.coldStartMs > 0)
  console.log(`[slice0] dsh web cold start: ${String(report.coldStartMs)} ms`)
  console.log(`[slice0] contiguous events: ${String(report.eventCount)} through cursor ${String(report.finalCursor)}`)
})
