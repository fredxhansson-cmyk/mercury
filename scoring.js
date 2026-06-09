/**
 * MERCURY — PRODUCT SCORING SYSTEM
 * 
 * Tracks clicks + conversions per product.
 * Calculates a score and writes it back to Shopify as metafields.
 * Called by: Express routes (click/conversion tracking) + cron job (score update).
 */

const fetch = require('node-fetch');

// ─── SCORE FORMULA ────────────────────────────────────────────────────────────
// score = (conversions × 10) + (clicks × 1) - (days_since_published × 0.3)
// New products get a +20 freshness boost that decays over 14 days.
// Result is clamped 0–1000 and written as custom.score metafield.

function calculateScore(clicks, conversions, publishedAt) {
  const daysSince = (Date.now() - new Date(publishedAt).getTime()) / 86400000;
  const freshBoost = Math.max(0, 20 - daysSince * (20 / 14)); // fades over 14 days
  const raw = (conversions * 10) + (clicks * 1) + freshBoost - (daysSince * 0.3);
  return Math.min(1000, Math.max(0, Math.round(raw)));
}

// ─── SHOPIFY METAFIELD WRITER ─────────────────────────────────────────────────
// Writes three metafields per product:
//   custom.score       → integer 0–1000  (for sorting)
//   custom.clicks      → integer         (display: "1 234 visningar")
//   custom.conversions → integer         (display: "89 köp")

async function writeProductMetafields(productId, score, clicks, conversions) {
  const shop  = process.env.SHOPIFY_SHOP_DOMAIN;  // e.g. vintera.myshopify.com
  const token = process.env.SHOPIFY_ACCESS_TOKEN; // needs write_products scope

  const metafields = [
    { namespace: 'custom', key: 'score',       value: String(score),       type: 'number_integer' },
    { namespace: 'custom', key: 'clicks',      value: String(clicks),      type: 'number_integer' },
    { namespace: 'custom', key: 'conversions', value: String(conversions), type: 'number_integer' },
  ];

  // Shopify allows batch metafield upsert via product update
  const res = await fetch(
    `https://${shop}/admin/api/2024-01/products/${productId}/metafields.json`,
    {
      method: 'POST',
      headers: {
        'Content-Type':         'application/json',
        'X-Shopify-Access-Token': token,
      },
      body: JSON.stringify({ metafield: metafields[0] }),
    }
  );

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Shopify metafield write failed for product ${productId}: ${err}`);
  }

  // Write clicks + conversions in parallel (fire-and-forget errors logged only)
  await Promise.allSettled(
    metafields.slice(1).map(mf =>
      fetch(`https://${shop}/admin/api/2024-01/products/${productId}/metafields.json`, {
        method: 'POST',
        headers: {
          'Content-Type':           'application/json',
          'X-Shopify-Access-Token': token,
        },
        body: JSON.stringify({ metafield: mf }),
      }).catch(e => console.error('[metafield]', e.message))
    )
  );
}

// ─── BULK SCORE UPDATE ────────────────────────────────────────────────────────
// Fetches all tracked products from DB, recalculates scores, writes to Shopify.
// Called by cron every 6 hours (see cron.js).

async function runScoreUpdate(db) {
  console.log('[scoring] Starting score update run...');

  let rows;
  try {
    rows = await db.all(`
      SELECT
        product_id,
        shopify_product_id,
        published_at,
        COALESCE(clicks, 0)      AS clicks,
        COALESCE(conversions, 0) AS conversions
      FROM product_stats
    `);
  } catch (e) {
    console.error('[scoring] DB read failed:', e.message);
    return;
  }

  console.log(`[scoring] Updating ${rows.length} products...`);
  let updated = 0;
  let failed  = 0;

  for (const row of rows) {
    try {
      const score = calculateScore(row.clicks, row.conversions, row.published_at);
      await writeProductMetafields(row.shopify_product_id, score, row.clicks, row.conversions);
      await db.run(
        'UPDATE product_stats SET last_score = ?, updated_at = CURRENT_TIMESTAMP WHERE shopify_product_id = ?',
        [score, row.shopify_product_id]
      );
      updated++;
    } catch (e) {
      console.error(`[scoring] Failed for product ${row.shopify_product_id}:`, e.message);
      failed++;
    }
  }

  console.log(`[scoring] Done. Updated: ${updated}, Failed: ${failed}`);
}

module.exports = { calculateScore, writeProductMetafields, runScoreUpdate };
