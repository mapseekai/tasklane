import { defineConfig } from '@playwright/test';
const browsers = (process.env.BROWSERS || 'chrome,firefox,webkit').split(',');
export default defineConfig({
  testDir: 'test/browser',
  timeout: 30000,
  workers: 1,
  retries: 0,
  use: { baseURL: 'http://127.0.0.1:4196' },
  reporter: [['list'], ['json', { outputFile: 'test-results/browser.json' }]],
  webServer: {
    command: 'node scripts/server.mjs',
    url: 'http://127.0.0.1:4196/test/browser/harness.html',
    reuseExistingServer: false,
  },
  projects: browsers.map((name) => ({
    name,
    use: name === 'chrome' ? { browserName: 'chromium', channel: 'chrome' } : { browserName: name },
  })),
});
