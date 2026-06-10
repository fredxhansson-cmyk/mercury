/**
 * MELONI — UPPDATERA KOLLEKTIONSBILDER
 * Hämtar professionella sportbilder från Unsplash
 * och sätter dem på varje kollektion i Shopify.
 * 
 * Kör manuellt: node updateCollectionImages.js
 * Eller via API: GET /api/update-collection-images
 */

require('dotenv').config();
const axios = require('axios');

const SHOP         = process.env.SHOPIFY_DOMAIN;
const TOKEN        = process.env.SHOP_TOKEN || process.env.SHOPIFY_TOKEN;
const UNSPLASH_KEY = process.env.UNSPLASH_ACCESS_KEY;

// Sökord per kollektion — väljer bland 30 slumpmässiga bilder för variation
const COLLECTION_IMAGES = [
  { handle: 'herr',                  query: 'mens athletic sportswear running outdoor' },
  { handle: 'dam',                   query: 'womens fitness sportswear active lifestyle' },
  { handle: 'barn',                  query: 'kids outdoor sport active children' },
  { handle: 'traning-fitness',       query: 'gym fitness training weights workout' },
  { handle: 'friluftsliv-outdoor',   query: 'hiking mountains outdoor adventure nature' },
  { handle: 'lopning',               query: 'running marathon trail road athlete' },
  { handle: 'yoga',                  query: 'yoga meditation mindfulness flexibility calm' },
  { handle: 'cykling',               query: 'cycling road bike mountain sport' },
  { handle: 'vandring',              query: 'hiking camping mountains tent nature' },
  { handle: 'smart-teknik',          query: 'smartwatch fitness tracker sport technology' },
  { handle: 'aterhämtning-halsa',    query: 'recovery massage wellness health sport' },
  { handle: 'kost-vatska',           query: 'nutrition protein sports drink healthy food' },
  { handle: 'utrustning-tillbehor',  query: 'sports equipment gym gear accessories' },
  { handle: 'livsstil',              query: 'active lifestyle sport casual outdoor' },
  { handle: 'nyheter',               query: 'new sport collection modern athletic' },
  { handle: 'bestsellers',           query: 'popular sport bestseller fitness' },
  { handle: 'rea',                   query: 'sport sale discount fitness deal' },
  // Herr underkategorier
  { handle: 'herr-loparskor',        query: 'mens running shoes road athlete' },
  { handle: 'herr-traningsskor',     query: 'mens training shoes gym workout' },
  { handle: 'herr-jackor',           query: 'mens sport jacket outdoor windproof' },
  { handle: 'herr-shorts',           query: 'mens running shorts training sport' },
  { handle: 'herr-hoodies',          query: 'mens sport hoodie sweatshirt gym' },
  // Dam underkategorier
  { handle: 'dam-loparskor',         query: 'womens running shoes athletic road' },
  { handle: 'dam-shorts',            query: 'womens running tights leggings sport' },
  { handle: 'dam-hoodies',           query: 'womens sport hoodie sweatshirt fitness' },
  { handle: 'dam-jackor',            query: 'womens outdoor jacket sport windproof' },
  { handle: 'sport-bh-hog',          query: 'womens sports bra running support fitness' },
  { handle: 'sport-bh-medium',       query: 'womens sports bra training yoga' },
  { handle: 'sport-bh-latt',         query: 'womens light sports bra yoga pilates' },
  // Barn
  { handle: 'barn-loparskor',        query: 'kids running shoes athletic children' },
  { handle: 'barn-jackor',           query: 'kids outdoor jacket rain sport children' },
];

async function getUnsplashImage(query) {
  if (!UNSPLASH_KEY) {
    console.log('  ⚠️  Ingen Unsplash-nyckel — hoppar över bilder');
    return null;
  }
  try {
    // Hämta slumpmässig bild från de 30 senaste resultaten för variation
    const page = Math.floor(Math.random() * 3) + 1;
    const res = await axios.get('https://api.unsplash.com/photos/random', {
      headers: { Authorization: `Client-ID ${UNSPLASH_KEY}` },
      params: {
        query,
        orientation: 'landscape',
        content_filter: 'high',
        count: 1,
      }
    });
    const photos = Array.isArray(res.data) ? res.data : [res.data];
    if (!photos.length) return null;
    const photo = photos[Math.floor(Math.random() * photos.length)];
    // Hög kvalitet, 1600px bred
    return photo.urls?.regular?.replace('w=1080', 'w=1600') || photo.urls?.regular || null;
  } catch(e) {
    console.error('  Unsplash fel:', e.response?.status, e.message);
    return null;
  }
}

async function getCollectionId(handle) {
  try {
    const res = await axios.get(
      `https://${SHOP}/admin/api/2024-01/custom_collections.json?handle=${handle}`,
      { headers: { 'X-Shopify-Access-Token': TOKEN } }
    );
    return res.data.custom_collections?.[0]?.id || null;
  } catch(e) {
    return null;
  }
}

async function updateCollectionImage(handle, imageUrl) {
  const colId = await getCollectionId(handle);
  if (!colId) {
    console.log(`  ⚠️  Kollektion ej funnen: ${handle}`);
    return false;
  }
  try {
    await axios.put(
      `https://${SHOP}/admin/api/2024-01/custom_collections/${colId}.json`,
      { custom_collection: { id: colId, image: { src: imageUrl } } },
      { headers: { 'X-Shopify-Access-Token': TOKEN, 'Content-Type': 'application/json' } }
    );
    return true;
  } catch(e) {
    console.error(`  Fel för ${handle}:`, e.response?.data?.errors || e.message);
    return false;
  }
}

async function main() {
  console.log('\nMELONI — Uppdaterar kollektionsbilder från Unsplash');
  console.log(`Butik: ${SHOP}`);
  console.log(`Unsplash: ${UNSPLASH_KEY ? '✓ aktiv' : '✗ saknas'}\n`);

  const delay = ms => new Promise(r => setTimeout(r, ms));
  let updated = 0, failed = 0, skipped = 0;

  for (const col of COLLECTION_IMAGES) {
    process.stdout.write(`${col.handle}... `);
    const imageUrl = await getUnsplashImage(col.query);
    if (!imageUrl) { skipped++; console.log('hoppad (ingen bild)'); await delay(200); continue; }
    const ok = await updateCollectionImage(col.handle, imageUrl);
    if (ok) { updated++; console.log('✓'); }
    else { failed++; console.log('✗'); }
    await delay(500); // Respektera rate limits
  }

  console.log(`\nKlart! Uppdaterade: ${updated} · Misslyckades: ${failed} · Hoppade: ${skipped}`);
}

// Kör om anropat direkt
if (require.main === module) {
  main().catch(console.error);
}

module.exports = { main };
