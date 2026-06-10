/**
 * MELONI — SKAPA ALLA KOLLEKTIONER
 * Kör: node createCollections.js
 */

require('dotenv').config();
const fetch = require('node-fetch');

const SHOP  = process.env.SHOPIFY_SHOP_DOMAIN;
const TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;
const UNSPLASH_KEY = process.env.UNSPLASH_ACCESS_KEY;

const COLLECTIONS = [
  // TOPPNIVÅ
  { title: 'Nyheter',      handle: 'nyheter',     sort: 'created-descending', q: 'sport fitness new arrival' },
  { title: 'Bästsäljare',  handle: 'bestsellers', sort: 'best-selling',       q: 'sport bestseller popular' },
  { title: 'Rea',          handle: 'rea',          sort: 'price-descending',   q: 'sport sale discount' },
  // KÖN
  { title: 'Herr', handle: 'herr', q: 'mens sportswear running gym' },
  { title: 'Dam',  handle: 'dam',  q: 'womens sportswear fitness leggings' },
  { title: 'Barn', handle: 'barn', q: 'kids sportswear outdoor active' },
  // HERR KLÄDER
  { title: 'Herr T-shirts',               handle: 'herr-t-shirts',           q: 'mens sports t-shirt workout' },
  { title: 'Herr Linnen',                 handle: 'herr-linnen',              q: 'mens tank top gym' },
  { title: 'Herr Hoodies och Sweatshirts',handle: 'herr-hoodies',            q: 'mens sports hoodie sweatshirt' },
  { title: 'Herr Funktionskläder',        handle: 'herr-funktionskl-der',    q: 'mens compression sport shirt' },
  { title: 'Herr Kompressionskläder',     handle: 'herr-kompressionskl-der', q: 'mens compression tights workout' },
  { title: 'Herr Shorts',                 handle: 'herr-shorts',              q: 'mens running shorts training' },
  { title: 'Herr Byxor och Joggers',      handle: 'herr-byxor',              q: 'mens jogger pants training' },
  { title: 'Herr Jackor',                 handle: 'herr-jackor',              q: 'mens sport jacket outdoor' },
  { title: 'Herr Underställ',             handle: 'herr-underst-ll',          q: 'mens thermal base layer winter sport' },
  { title: 'Herr Strumpor och Underkläder',handle:'herr-strumpor',            q: 'mens sport socks athletic' },
  // HERR SKOR
  { title: 'Herr Löparskor',       handle: 'herr-loparskor',    q: 'mens running shoes road' },
  { title: 'Herr Träningsskor',    handle: 'herr-traningsskor', q: 'mens training shoes gym' },
  { title: 'Herr Trailskor',       handle: 'herr-trailskor',    q: 'mens trail running shoes mountain' },
  { title: 'Herr Vandringskängor', handle: 'herr-vandring',     q: 'mens hiking boots mountain' },
  { title: 'Herr Sandaler',        handle: 'herr-sandaler',     q: 'mens outdoor sandals sport' },
  // DAM KLÄDER
  { title: 'Dam T-shirts och Linnen',    handle: 'dam-t-shirts',           q: 'womens sports top fitness' },
  { title: 'Dam Hoodies och Sweatshirts',handle: 'dam-hoodies',            q: 'womens sports hoodie' },
  { title: 'Dam Funktionskläder',        handle: 'dam-funktionskl-der',    q: 'womens compression sport top' },
  { title: 'Dam Kompressionskläder',     handle: 'dam-kompressionskl-der', q: 'womens compression leggings workout' },
  { title: 'Dam Shorts och Tights',      handle: 'dam-shorts',              q: 'womens running shorts tights' },
  { title: 'Dam Byxor och Leggings',     handle: 'dam-byxor',              q: 'womens yoga leggings fitness' },
  { title: 'Dam Jackor',                 handle: 'dam-jackor',              q: 'womens sport jacket outdoor' },
  { title: 'Dam Underställ',             handle: 'dam-underst-ll',          q: 'womens thermal base layer winter' },
  // SPORT-BH
  { title: 'Sport-BH Latt Support',   handle: 'sport-bh-latt',   q: 'womens light support sports bra yoga' },
  { title: 'Sport-BH Medium Support', handle: 'sport-bh-medium', q: 'womens medium support sports bra' },
  { title: 'Sport-BH Hog Support',    handle: 'sport-bh-hog',    q: 'womens high impact sports bra running' },
  // DAM SKOR
  { title: 'Dam Löparskor',       handle: 'dam-loparskor',    q: 'womens running shoes' },
  { title: 'Dam Träningsskor',    handle: 'dam-traningsskor', q: 'womens training shoes gym' },
  { title: 'Dam Vandringskängor', handle: 'dam-vandring',     q: 'womens hiking boots' },
  // BARN KLÄDER
  { title: 'Barn T-shirts och Hoodies', handle: 'barn-t-shirts',  q: 'kids sports clothing active' },
  { title: 'Barn Shorts och Byxor',     handle: 'barn-shorts',    q: 'kids training shorts pants' },
  { title: 'Barn Jackor och Regnkläder',handle: 'barn-jackor',    q: 'kids outdoor jacket rain' },
  { title: 'Barn Outdoorkläder',        handle: 'barn-outdoor',   q: 'kids outdoor adventure clothing' },
  { title: 'Barn Underställ',           handle: 'barn-underst-ll', q: 'kids thermal underwear winter sport' },
  // BARN SKOR
  { title: 'Barn Löparskor',       handle: 'barn-loparskor',    q: 'kids running shoes athletic' },
  { title: 'Barn Träningsskor',    handle: 'barn-traningsskor', q: 'kids training shoes gym' },
  { title: 'Barn Outdoorskor',     handle: 'barn-outdoorskor',  q: 'kids outdoor shoes trail' },
  { title: 'Barn Vandringskängor', handle: 'barn-vandring',     q: 'kids hiking boots outdoor' },
  // SPORT
  { title: 'Träning och Fitness',    handle: 'traning-fitness',       q: 'gym training fitness equipment' },
  { title: 'Friluftsliv och Outdoor',handle: 'friluftsliv-outdoor',   q: 'outdoor hiking nature adventure' },
  { title: 'Löpning',                handle: 'lopning',                q: 'running marathon road trail' },
  { title: 'Yoga',                   handle: 'yoga',                   q: 'yoga mat meditation flexibility' },
  { title: 'Cykling',                handle: 'cykling',                q: 'cycling bike sport road' },
  { title: 'Vandring och Camping',   handle: 'vandring',               q: 'hiking mountain camping tent' },
  { title: 'Smart Teknik',           handle: 'smart-teknik',           q: 'fitness tracker smartwatch sport tech' },
  { title: 'Aterhamtning och Halsa', handle: 'aterhämtning-halsa',    q: 'recovery massage foam roller wellness' },
  { title: 'Kost och Vatska',        handle: 'kost-vatska',            q: 'protein nutrition water bottle sports' },
  { title: 'Utrustning och Tillbehor',handle:'utrustning-tillbehor',  q: 'sports equipment accessories gym bag' },
  { title: 'Livsstil',               handle: 'livsstil',               q: 'active lifestyle sport casual' },
];

async function getUnsplashImage(query) {
  if (!UNSPLASH_KEY) return null;
  try {
    const res = await fetch(
      `https://api.unsplash.com/photos/random?query=${encodeURIComponent(query)}&orientation=landscape&content_filter=high`,
      { headers: { Authorization: `Client-ID ${UNSPLASH_KEY}` } }
    );
    if (!res.ok) return null;
    const data = await res.json();
    return data.urls?.regular || null;
  } catch { return null; }
}

async function createCollection(col) {
  const checkRes = await fetch(
    `https://${SHOP}/admin/api/2024-01/custom_collections.json?handle=${col.handle}`,
    { headers: { 'X-Shopify-Access-Token': TOKEN } }
  );
  const checkData = await checkRes.json();
  if (checkData.custom_collections?.length > 0) {
    process.stdout.write(`  ✓ Finns: ${col.title}\n`);
    return checkData.custom_collections[0].id;
  }

  const imageUrl = await getUnsplashImage(col.q);
  const body = {
    custom_collection: {
      title: col.title, handle: col.handle,
      published: true, sort_order: col.sort || 'best-selling', body_html: '',
    }
  };
  if (imageUrl) body.custom_collection.image = { src: imageUrl };

  const res = await fetch(`https://${SHOP}/admin/api/2024-01/custom_collections.json`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': TOKEN },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  if (data.custom_collection) {
    process.stdout.write(`  + ${imageUrl ? '🖼️' : '📁'} Skapad: ${col.title}\n`);
    return data.custom_collection.id;
  } else {
    process.stdout.write(`  ✗ Fel: ${col.title} — ${JSON.stringify(data.errors)}\n`);
    return null;
  }
}

async function main() {
  console.log(`\nMELONI — Skapar ${COLLECTIONS.length} kollektioner\nButik: ${SHOP}\nUnsplash: ${UNSPLASH_KEY ? 'aktiv' : 'ej konfigurerat'}\n`);
  let ok = 0, fail = 0;
  for (const col of COLLECTIONS) {
    try {
      const id = await createCollection(col);
      if (id) ok++; else fail++;
      await new Promise(r => setTimeout(r, 350));
    } catch(e) {
      console.error(`  ✗ ${col.title}:`, e.message); fail++;
    }
  }
  console.log(`\nKlart! OK: ${ok} · Fel: ${fail}`);
  console.log('\nNästa: Gå till temaeditorn och koppla Trending Nu till en kollektion.');
}

main().catch(console.error);
