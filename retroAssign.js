/**
 * MERCURY — Retroaktiv kollektionstilldelning
 * 
 * Tilldelar alla befintliga Shopify-produkter till rätt kollektion.
 * Kör en gång: node retroAssign.js
 */

require('dotenv').config();
const axios = require('axios');

process.env.SHOPIFY_SHOP_DOMAIN = process.env.SHOPIFY_SHOP_DOMAIN || process.env.SHOPIFY_DOMAIN;
process.env.SHOPIFY_ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN || process.env.SHOP_TOKEN || process.env.SHOPIFY_TOKEN;

const { assignCollections } = require('./collectionAssign');
const { Pool } = require('pg');

async function run() {
  const shop  = process.env.SHOPIFY_SHOP_DOMAIN;
  const token = process.env.SHOPIFY_ACCESS_TOKEN;

  console.log('Fetching all products from Shopify...');

  // Get all products from Shopify
  const res = await axios.get(
    `https://${shop}/admin/api/2024-01/products.json?limit=250&fields=id,title,tags,vendor`,
    { headers: { 'X-Shopify-Access-Token': token } }
  );

  const products = res.data.products || [];
  console.log(`Found ${products.length} products in Shopify\n`);

  let success = 0, failed = 0;

  for (const p of products) {
    const tags = p.tags ? p.tags.split(',').map(t => t.trim()) : [];
    try {
      await assignCollections(p.id, p.title, tags, null);
      console.log(`✓ ${p.title}`);
      success++;
    } catch(e) {
      console.error(`✗ ${p.title}: ${e.message}`);
      failed++;
    }
    // Small delay to avoid rate limits
    await new Promise(r => setTimeout(r, 300));
  }

  console.log(`\n✅ Done! ${success} assigned, ${failed} failed`);
}

run().catch(console.error);
