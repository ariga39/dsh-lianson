import assert from 'node:assert/strict'

const WRITER_HELD = /session\/writer-held|writer[- ]held|already in use|已被占用/iu

export function assertNoWriterHeld({ consoleMessages, uiTexts, label }) {
  const messages = consoleMessages.map(entry => `${entry.type}: ${entry.text}`)
  const evidence = [...uiTexts, ...messages]
  assert.deepEqual(
    evidence.filter(text => WRITER_HELD.test(text)),
    [],
    `${label}: session/writer-held must not appear in console or UI warnings/toasts`,
  )
}

export async function assertNoWriterHeldInPage(page, consoleMessages, label) {
  await page.waitForTimeout(250)
  const uiTexts = await page.evaluate(() => {
    const observed = Array.isArray(window.__dshObservedNotices) ? window.__dshObservedNotices : []
    const visible = [...document.querySelectorAll('[role="alert"], [role="status"], [aria-live]')]
      .filter(element => {
        const style = getComputedStyle(element)
        const rect = element.getBoundingClientRect()
        return style.display !== 'none' && style.visibility !== 'hidden' && rect.width > 0 && rect.height > 0
      })
      .map(element => element.textContent ?? '')
    return [...new Set([...observed, ...visible])]
  })
  assertNoWriterHeld({ consoleMessages, uiTexts, label })
}

export async function openSessionInBrowser({ browser, launchUrl, sessionId, title, workspaceTitle, pageErrors, consoleMessages }) {
  const page = await browser.newPage({ locale: 'en-US', viewport: { width: 1680, height: 1000 } })
  page.on('pageerror', error => pageErrors.push(error))
  page.on('console', message => consoleMessages.push({ type: message.type(), text: message.text() }))
  await page.addInitScript(() => {
    window.__dshObservedNotices = []
    const record = () => {
      for (const element of document.querySelectorAll('[role="alert"], [role="status"], [aria-live]')) {
        const text = element.textContent ?? ''
        if (text !== '' && !window.__dshObservedNotices.includes(text)) window.__dshObservedNotices.push(text)
      }
    }
    const observer = new MutationObserver(record)
    observer.observe(document, { childList: true, subtree: true, characterData: true })
    document.addEventListener('DOMContentLoaded', record, { once: true })
    record()
  })
  await page.goto(launchUrl, { waitUntil: 'load', timeout: 60_000 })
  await page.waitForSelector('[class*="frame"]', { timeout: 30_000 })

  const welcome = page.getByRole('button', { name: 'Continue', exact: true })
  try {
    await welcome.waitFor({ state: 'visible', timeout: 15_000 })
    await welcome.click()
  } catch {
    // The notice is versioned; an already-acknowledged home renders no modal.
  }

  const row = page.getByRole('treeitem').filter({ hasText: title }).first()
  const deadline = Date.now() + 30_000
  while (Date.now() < deadline) {
    if (await row.isVisible().catch(() => false)) break
    // A Workspace row is expandable and carries aria-expanded; click only the
    // collapsed row so the poll cannot toggle an already-open group shut.
    const collapsed = page.locator('[role="treeitem"][aria-expanded="false"]').filter({ hasText: workspaceTitle }).first()
    if (await collapsed.count() > 0) await collapsed.click().catch(() => {})
    await new Promise(resolve => setTimeout(resolve, 250))
  }
  await row.waitFor({ state: 'visible', timeout: 30_000 })
  await row.click()
  await page.waitForFunction(
    id => {
      try {
        return JSON.parse(localStorage.getItem('dsh.sessions.current') ?? 'null')?.sessionId === id
      } catch {
        return false
      }
    },
    sessionId,
    { timeout: 30_000 },
  )
  return page
}
