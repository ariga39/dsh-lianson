export async function openSessionInBrowser({ browser, launchUrl, sessionId, title, workspaceTitle, pageErrors }) {
  const page = await browser.newPage({ locale: 'en-US', viewport: { width: 1680, height: 1000 } })
  page.on('pageerror', error => pageErrors.push(error))
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
  const workspaceRow = page.getByRole('treeitem').filter({ hasText: workspaceTitle }).first()
  const deadline = Date.now() + 30_000
  while (Date.now() < deadline) {
    if (await row.isVisible().catch(() => false)) break
    if (await workspaceRow.isVisible().catch(() => false)) await workspaceRow.click().catch(() => {})
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
