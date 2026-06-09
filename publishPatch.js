/**
 * MERCURY — publishProduct PATCH
 * 
 * Lägg till dessa anrop i din befintliga publishProduct()-funktion
 * direkt efter att Shopify-produkten skapats.
 * 
 * ─── HUR DU INTEGRERAR ───────────────────────────────────────────────────────
 * 
 * Din nuvarande publishProduct() ser förmodligen ut ungefär så här:
 * 
 *   async function publishProduct(productData) {
 *     const shopifyProduct = await createShopifyProduct(productData);
 *     // ... resten av logiken
 *   }
 * 
 * Lägg till raderna markerade med [LÄGG TILL] nedan.
 */

// [LÄGG TILL] Importera i toppen av din server.js / productService.js:
// const { registerNewProduct } = require('./publishPatch');

const fetch = require('node-fetch');

/**
 * Registrerar en nyligen publicerad produkt i scoring-systemet.
 * Anropas automatiskt av publishProduct() efter Shopify-skapandet.
 * 
 * @param {string} shopifyProductId  - Shopifys produkt-ID (från API-svaret)
 * @param {string} productTitle      - Produktens titel (för läsbarhet i dashboard)
 * @param {object} db                - SQLite-instansen (skickas från server.js)
 */
async function registerNewProduct(shopifyProductId, productTitle, db) {
  try {
    // 1. Registrera i lokal DB för tracking
    await db.run(`
      INSERT INTO product_stats (shopify_product_id, product_title, published_at)
      VALUES (?, ?, datetime('now'))
      ON CONFLICT (shopify_product_id) DO NOTHING
    `, [String(shopifyProductId), productTitle || '']);

    // 2. Sätt initial score = 20 (freshness boost) direkt i Shopify
    //    så produkten syns i "Trending Nu"-sektionen redan från start
    const initialScore = 20;
    const shop  = process.env.SHOPIFY_SHOP_DOMAIN;
    const token = process.env.SHOPIFY_ACCESS_TOKEN;

    const metafields = [
      { namespace: 'custom', key: 'score',       value: String(initialScore), type: 'number_integer' },
      { namespace: 'custom', key: 'clicks',      value: '0',                  type: 'number_integer' },
      { namespace: 'custom', key: 'conversions', value: '0',                  type: 'number_integer' },
    ];

    await Promise.allSettled(
      metafields.map(mf =>
        fetch(`https://${shop}/admin/api/2024-01/products/${shopifyProductId}/metafields.json`, {
          method:  'POST',
          headers: {
            'Content-Type':           'application/json',
            'X-Shopify-Access-Token': token,
          },
          body: JSON.stringify({ metafield: mf }),
        }).then(r => {
          if (!r.ok) throw new Error(`HTTP ${r.status}`);
        })
      )
    );

    console.log(`[scoring] Registered product ${shopifyProductId} ("${productTitle}") with score ${initialScore}`);
  } catch (e) {
    // Non-fatal — product still published, scoring can catch up on next cron run
    console.error(`[scoring] Failed to register product ${shopifyProductId}:`, e.message);
  }
}

module.exports = { registerNewProduct };

/* ─── INTEGRATION EXAMPLE ────────────────────────────────────────────────────
 *
 * I din befintliga publishProduct-funktion, lägg till [LÄGG TILL]-raderna:
 *
 *   const { registerNewProduct } = require('./publishPatch');  // [LÄGG TILL]
 *
 *   async function publishProduct(productData, db) {           // [LÄGG TILL db-parameter]
 *     // ... din befintliga Shopify API-kod ...
 *     const shopifyProduct = await createShopifyProduct(productData);
 *
 *     // [LÄGG TILL] direkt efter Shopify-skapandet:
 *     await registerNewProduct(
 *       shopifyProduct.id,
 *       shopifyProduct.title,
 *       db
 *     );
 *
 *     return shopifyProduct;
 *   }
 */
