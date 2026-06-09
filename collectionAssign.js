/**
 * MERCURY — KOLLEKTIONSINTEGRERING
 * 
 * Lägg till i din publishProduct()-funktion för att automatiskt
 * lägga produkter i rätt kollektion baserat på titel + taggar.
 * 
 * ─── INTEGRATION ─────────────────────────────────────────────
 * 
 * I din publishProduct()-funktion, efter att produkten skapats:
 * 
 *   const { assignCollections } = require('./collectionAssign');
 *   await assignCollections(shopifyProductId, product.title, product.tags, db);
 */

const fetch = require('node-fetch');
const { matchCollections, getCollectionId } = require('./collections');

/**
 * Lägger till en produkt i matchande kollektioner i Shopify.
 * 
 * OBS: Kräver att shopify_id är ifyllt i collections.js.
 * Kör först: node setupCollections.js för att skapa + spara ID:n.
 */
async function assignCollections(shopifyProductId, title, tags, db) {
  const shop  = process.env.SHOPIFY_SHOP_DOMAIN;
  const token = process.env.SHOPIFY_ACCESS_TOKEN;

  const handles = matchCollections(title, tags);
  const results = [];

  for (const handle of handles) {
    const colId = getCollectionId(handle);

    if (!colId) {
      console.warn(`[collections] Inget Shopify-ID för ${handle} — fyll i collections.js`);
      continue;
    }

    try {
      const res = await fetch(
        `https://${shop}/admin/api/2024-01/collects.json`,
        {
          method: 'POST',
          headers: {
            'Content-Type':           'application/json',
            'X-Shopify-Access-Token': token,
          },
          body: JSON.stringify({
            collect: {
              product_id:    shopifyProductId,
              collection_id: colId,
            }
          }),
        }
      );

      if (res.ok) {
        results.push({ handle, success: true });
        console.log(`[collections] ${title} → ${handle} ✓`);
      } else {
        const err = await res.json();
        // 422 = already in collection, not a real error
        if (res.status !== 422) {
          console.error(`[collections] ${handle}: HTTP ${res.status}`, err);
        }
        results.push({ handle, success: res.status === 422 });
      }
    } catch (e) {
      console.error(`[collections] ${handle}:`, e.message);
      results.push({ handle, success: false, error: e.message });
    }
  }

  return results;
}

/**
 * Engångsskript: Skapar alla 7 kollektioner i Shopify och sparar deras ID:n.
 * Kör en gång: node setupCollections.js
 */
async function setupCollections() {
  const { COLLECTIONS } = require('./collections');
  const shop  = process.env.SHOPIFY_SHOP_DOMAIN;
  const token = process.env.SHOPIFY_ACCESS_TOKEN;
  const fs    = require('fs');
  const path  = require('path');

  console.log('Skapar kollektioner i Shopify...\n');

  const updates = [];

  for (const col of COLLECTIONS) {
    try {
      // Kontrollera om kollektionen redan finns
      const searchRes = await fetch(
        `https://${shop}/admin/api/2024-01/custom_collections.json?handle=${col.handle}`,
        { headers: { 'X-Shopify-Access-Token': token } }
      );
      const searchData = await searchRes.json();

      let shopifyId;

      if (searchData.custom_collections && searchData.custom_collections.length > 0) {
        shopifyId = searchData.custom_collections[0].id;
        console.log(`✓ Hittades redan: ${col.name} (ID: ${shopifyId})`);
      } else {
        // Skapa ny kollektion
        const createRes = await fetch(
          `https://${shop}/admin/api/2024-01/custom_collections.json`,
          {
            method: 'POST',
            headers: {
              'Content-Type':           'application/json',
              'X-Shopify-Access-Token': token,
            },
            body: JSON.stringify({
              custom_collection: {
                title:        col.name,
                handle:       col.handle,
                published:    true,
                sort_order:   'best-selling',
                body_html:    '',
              }
            }),
          }
        );
        const createData = await createRes.json();
        shopifyId = createData.custom_collection?.id;
        console.log(`+ Skapad: ${col.name} (ID: ${shopifyId})`);
      }

      updates.push({ handle: col.handle, shopify_id: shopifyId });
    } catch (e) {
      console.error(`✗ Fel för ${col.name}:`, e.message);
    }
  }

  // Skriv tillbaka ID:n till collections.js
  let collectionsFile = fs.readFileSync(path.join(__dirname, 'collections.js'), 'utf8');

  for (const { handle, shopify_id } of updates) {
    if (!shopify_id) continue;
    const regex = new RegExp(
      `(handle:\\s*'${handle}',[^}]*shopify_id:\\s*)null`,
      's'
    );
    collectionsFile = collectionsFile.replace(regex, `$1${shopify_id}`);
  }

  fs.writeFileSync(path.join(__dirname, 'collections.js'), collectionsFile);
  console.log('\n✅ collections.js uppdaterad med Shopify-ID:n');
  console.log('Nu kan Mercury automatiskt lägga produkter i rätt kollektion.');
}

// Kör setup om anropat direkt: node collectionAssign.js setup
if (require.main === module) {
  require('dotenv').config();
  setupCollections().catch(console.error);
}

module.exports = { assignCollections, setupCollections };
