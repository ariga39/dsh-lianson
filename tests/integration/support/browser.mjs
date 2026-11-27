/** Real `dsh Web` observation through Playwright. */

import { basename } from 'node:path'

/**
 * Open the authenticated page, locate the workspace and session by title, and
 * confirm the client selected the exact session id.
 * @param {object} input
 * @param {import('playwright').Browser} input.browser
 * @param {string} input.launchUrl
 * @param {string} input.sessionId
 * @param {string} input.title
 * @param {string} input.cwd
 * @param {Error[]} input.pageErrors
 * @returns {Promise<import('playwright').Page>}
 */
export async function openSessionInBrowser({ browser, launchUrl, sessionId, title, cwd, pageErrors }) {
  const page = await browser.newPage({ locale: 'en-US', viewport: { width: 1680, height: 1000 } })
  page.on('pageerror', error => pageErrors.push(error))
  await page.goto(launchUrl, { waitUntil: 'load', timeout: 60_000 })
  await page.waitForSelector('[class*="frame"]', { timeout: 30_000 })

  const welcome = page.getByRole('button', { name: 'Continue', exact: true })
  try {
    await welcome.waitFor({ state: 'visible', timeout: 10_000 })
    await welcome.click()
  } catch {
    // The notice is versioned; an acknowledged home renders no modal.
  }

  const workspaceRow = page.getByRole('treeitem').filter({ hasText: basename(cwd) }).first()
  const row = page.getByRole('treeitem').filter({ hasText: title }).first()
  const deadline = Date.now() + 30_000
  while (Date.now() < deadline) {
    if (await row.isVisible().catch(() => false)) break
    if (await workspaceRow.isVisible().catch(() => false)) await workspaceRow.click().catch(() => {})
    await new Promise(resolve => setTimeout(resolve, 250))
  }
  await row.waitFor({ state: 'visible', timeout: 30_000 })
  await row.click()
  await page.waitForFunction(
    (id) => {
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
