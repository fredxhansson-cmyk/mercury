/**
 * MERCURY — Ta bort irrelevanta produkter från Shopify
 * 
 * Tar bort produkter som matchar blocklistan.
 * Kör: node cleanProducts.js
 * 
 * OBS: Kör med --dry-run för att se vad som skulle tas bort utan att faktiskt ta bort.
 * node cleanProducts.js --dry-run
 */

require('dotenv').config();
const axios = require('axios');

process.env.SHOPIFY_SHOP_DOMAIN = process.env.SHOPIFY_SHOP_DOMAIN || process.env.SHOPIFY_DOMAIN;
process.env.SHOPIFY_ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN || process.env.SHOP_TOKEN || process.env.SHOPIFY_TOKEN;

const shop  = process.env.SHOPIFY_SHOP_DOMAIN;
const token = process.env.SHOPIFY_ACCESS_TOKEN;
const dryRun = process.argv.includes('--dry-run');

const BLOCKED_KEYWORDS = [
  'bra', 'bralette', 'lingerie', 'underwear', 'panties', 'bikini bottom',
  'car mat', 'floor mat clip', 'speedometer', 'ford', 'mustang', 'toyota',
  'honda', 'bmw', 'mercedes', 'audi', 'volkswagen', 'nissan', 'hyundai',
  'whiskey', 'whisky', 'beer', 'wine', 'vodka', 'alcohol',
  'cigarette', 'tobacco', 'vape',
  'pet dress', 'dog dress', 'cat dress',
  'casino', 'gambling', 'betting',
];

function shouldRemove(title) {
  const lower = title.toLowerCase();
  return BLOCKED_KEYWORDS.some(kw => lower.includes(kw));
}

async function run() {
  console.log(dryRun ? '🔍 DRY RUN — no products will be deleted\n' : '⚠️  LIVE RUN — products will be deleted\n');

  const res = await axios.get(
    `https://${shop}/admin/api/2024-01/products.json?limit=250&fields=id,title`,
    { headers: { 'X-Shopify-Access-Token': token } }
  );

  const products = res.data.products || [];
  console.log(`Found ${products.length} products\n`);

  let removed = 0, kept = 0;

  for (const p of products) {
    if (shouldRemove(p.title)) {
      console.log(`${dryRun ? '🗑️  Would remove' : '🗑️  Removing'}: ${p.title}`);
      if (!dryRun) {
        try {
          await axios.delete(
            `https://${shop}/admin/api/2024-01/products/${p.id}.json`,
            { headers: { 'X-Shopify-Access-Token': token } }
          );
          await new Promise(r => setTimeout(r, 500));
        } catch(e) {
          console.error(`  Failed: ${e.message}`);
        }
      }
      removed++;
    } else {
      kept++;
    }
  }

  console.log(`\n✅ Done! ${removed} ${dryRun ? 'would be removed' : 'removed'}, ${kept} kept`);
}

run().catch(console.error);
