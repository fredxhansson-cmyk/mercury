/**
 * MERCURY — CRON JOB
 * Recalculates product scores and writes metafields to Shopify every 6 hours.
 * 
 * Mount in server.js:
 *   require('./cron')(db);
 */

const { runScoreUpdate } = require('./scoring');

module.exports = function startCron(db) {
  const SIX_HOURS = 6 * 60 * 60 * 1000;

  async function run() {
    try {
      await runScoreUpdate(db);
    } catch (e) {
      console.error('[cron] Score update crashed:', e.message);
    }
  }

  // Run once immediately on startup (catches any products added while server was down)
  setTimeout(run, 10_000);

  // Then every 6 hours
  setInterval(run, SIX_HOURS);

  console.log('[cron] Score updater scheduled every 6 hours.');
};
