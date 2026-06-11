require('dotenv').config();
const express = require('express');
const cors = require('cors');
const cron = require('node-cron');
const axios = require('axios');
const OpenAI = require('openai');
const { Pool } = require('pg');

// Tracking & scoring (from Vintera integration)
let trackingRoutes, startScoreCron, registerNewProduct;
try {
  trackingRoutes = require('./trackingRoutes');
  startScoreCron = require('./cron');
  registerNewProduct = require('./publishPatch').registerNewProduct;
  console.log('✓ Tracking modules loaded');
} catch(e) {
  console.log('Tracking modules not found — skipping');
}

// ── DATABASE ───────────────────────────────────────────────
let db = null;
process.env.SHOPIFY_SHOP_DOMAIN = process.env.SHOPIFY_SHOP_DOMAIN || process.env.SHOPIFY_DOMAIN;
process.env.SHOPIFY_ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN || process.env.SHOP_TOKEN || process.env.SHOPIFY_TOKEN;

async function initDB() {
  if (!process.env.DATABASE_URL) {
    console.log('No DATABASE_URL — using in-memory storage');
    return;
  }
  try {
    db = new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } });
    await db.query(`
      CREATE TABLE IF NOT EXISTS queue (
        id TEXT PRIMARY KEY,
        data JSONB NOT NULL,
        created_at TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS products (
        id TEXT PRIMARY KEY,
        data JSONB NOT NULL,
        approved_at TIMESTAMP DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS removed (
        id TEXT PRIMARY KEY,
        data JSONB NOT NULL,
        removed_at TIMESTAMP DEFAULT NOW()
      );
    `);
    console.log('✓ Database connected and tables ready');
    const q = await db.query('SELECT data FROM queue ORDER BY created_at DESC');
    const p = await db.query('SELECT data FROM products ORDER BY approved_at DESC');
    store.queue = q.rows.map(r => r.data);
    store.products = p.rows.map(r => r.data);
    console.log(`Loaded ${store.queue.length} queued, ${store.products.length} approved products from DB`);
  } catch(e) {
    console.error('DB init failed — using in-memory:', e.message);
    db = null;
  }
}

async function dbSave(table, item) {
  if (!db) return;
  try {
    await db.query(
      `INSERT INTO ${table} (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data = $2`,
      [item.id, JSON.stringify(item)]
    );
  } catch(e) { console.error('DB save failed:', e.message); }
}

async function dbDelete(table, id) {
  if (!db) return;
  try {
    await db.query(`DELETE FROM ${table} WHERE id = $1`, [id]);
  } catch(e) { console.error('DB delete failed:', e.message); }
}

const app = express();
app.use(cors());
app.use(express.json());

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

let store = {
  products: [],
  queue: [],
  removed: [],
  lastSync: null,
  syncStatus: 'idle',
  stats: { totalScanned: 0, totalQueued: 0, totalApproved: 0, totalRejected: 0 }
};

// ── SMART PRICE SNAPPING ───────────────────────────────────
const SNAP_POINTS = [
  29, 39, 49, 59, 79, 99, 119, 149, 179, 199,
  229, 249, 299, 349, 399, 449, 499, 549, 599,
  699, 799, 899, 999, 1099, 1199, 1299, 1499, 1999, 2499, 2999
];

function snapPrice(rawSek) {
  if (rawSek <= 0) return 49;
  let closest = SNAP_POINTS[0];
  let minDiff = Math.abs(rawSek - SNAP_POINTS[0]);
  for (const p of SNAP_POINTS) {
    const diff = Math.abs(rawSek - p);
    if (diff < minDiff) { minDiff = diff; closest = p; }
  }
  return closest;
}

// ── OUTDOOR-FIRST KEYWORD LISTS ────────────────────────────
// These keywords ALWAYS map to Friluftsliv & Outdoor — never to Träning & Fitness.
const OUTDOOR_PRIORITY_KEYWORDS = [
  'tent', 'tents', 'camping tent', 'dome tent', 'backpacking tent', 'shelter', 'tarp', 'bivvy', 'hammock tent',
  'sleeping bag', 'sleep bag', 'mummy bag', 'down sleeping', 'quilt sleeping',
  'trekking poles', 'trekking pole', 'hiking poles', 'hiking pole', 'walking poles', 'trail poles',
  'hiking boots', 'trail boots', 'approach shoes', 'hiking shoes', 'waterproof boots',
  'hiking backpack', 'trekking backpack', 'trail backpack', 'outdoor backpack', 'camping backpack',
  'daypack', 'summit pack',
  'camping stove', 'camp stove', 'backpacking stove', 'camping cookware', 'camp cookset',
  'camping pot', 'titanium pot', 'mess kit',
  'headlamp', 'head lamp', 'camping lantern', 'camp light',
  'compass', 'orienteering', 'survival kit', 'emergency blanket', 'fire starter', 'flint',
  'carabiner', 'climbing rope', 'harness', 'belay',
  'water filter', 'water purifier', 'water purification', 'life straw',
  'camping', 'camp gear', 'campsite', 'campfire', 'outdoor cooking',
  'hiking', 'trekking', 'mountaineering',
  'backpacking', 'bikepacking', 'kayaking', 'canoe', 'paddling',
  'rock climbing', 'bouldering', 'rappelling',
  'snowshoeing', 'snowshoe', 'ski touring', 'backcountry',
];

const CATEGORY_MAP = {
  'hiking':         { sv: 'Friluftsliv & Outdoor', tag: 'vandring',      shopify: 'Friluftsliv & Outdoor', gender: 'unisex' },
  'outdoor':        { sv: 'Friluftsliv & Outdoor', tag: 'outdoor',       shopify: 'Friluftsliv & Outdoor', gender: 'unisex' },
  'camping':        { sv: 'Friluftsliv & Outdoor', tag: 'camping',       shopify: 'Friluftsliv & Outdoor', gender: 'unisex' },
  'trekking':       { sv: 'Friluftsliv & Outdoor', tag: 'vandring',      shopify: 'Friluftsliv & Outdoor', gender: 'unisex' },
  'climbing':       { sv: 'Friluftsliv & Outdoor', tag: 'klättring',     shopify: 'Friluftsliv & Outdoor', gender: 'unisex' },
  'trail':          { sv: 'Friluftsliv & Outdoor', tag: 'trail',         shopify: 'Friluftsliv & Outdoor', gender: 'unisex' },
  'backpacking':    { sv: 'Friluftsliv & Outdoor', tag: 'outdoor',       shopify: 'Friluftsliv & Outdoor', gender: 'unisex' },
  'running':        { sv: 'Löpning',               tag: 'löpning',       shopify: 'Löpning',               gender: 'unisex' },
  'cycling':        { sv: 'Cykling',               tag: 'cykling',       shopify: 'Cykling',               gender: 'unisex' },
  'cycling computer':{ sv: 'Cykling',              tag: 'cykling',       shopify: 'Cykling',               gender: 'unisex' },
  'yoga':           { sv: 'Yoga',                  tag: 'yoga',          shopify: 'Yoga',                  gender: 'dam'    },
  'gym':            { sv: 'Träning & Fitness',      tag: 'gym',           shopify: 'Träning & Fitness',     gender: 'unisex' },
  'fitness':        { sv: 'Träning & Fitness',      tag: 'fitness',       shopify: 'Träning & Fitness',     gender: 'unisex' },
  'workout':        { sv: 'Träning & Fitness',      tag: 'träning',       shopify: 'Träning & Fitness',     gender: 'unisex' },
  'training':       { sv: 'Träning & Fitness',      tag: 'träning',       shopify: 'Träning & Fitness',     gender: 'unisex' },
  'resistance':     { sv: 'Träning & Fitness',      tag: 'styrketräning', shopify: 'Träning & Fitness',     gender: 'unisex' },
  'crossfit':       { sv: 'Träning & Fitness',      tag: 'crossfit',      shopify: 'Träning & Fitness',     gender: 'unisex' },
  'weightlifting':  { sv: 'Träning & Fitness',      tag: 'styrketräning', shopify: 'Träning & Fitness',     gender: 'herr'   },
  'cardio':         { sv: 'Träning & Fitness',      tag: 'kondition',     shopify: 'Träning & Fitness',     gender: 'unisex' },
  'exercise':       { sv: 'Träning & Fitness',      tag: 'träning',       shopify: 'Träning & Fitness',     gender: 'unisex' },
  'sports':         { sv: 'Träning & Fitness',      tag: 'sport',         shopify: 'Träning & Fitness',     gender: 'unisex' },
  'recovery':       { sv: 'Återhämtning & Hälsa',  tag: 'återhämtning',  shopify: 'Återhämtning & Hälsa', gender: 'unisex' },
  'massage':        { sv: 'Återhämtning & Hälsa',  tag: 'massage',       shopify: 'Återhämtning & Hälsa', gender: 'unisex' },
  'brace':          { sv: 'Återhämtning & Hälsa',  tag: 'skydd',         shopify: 'Återhämtning & Hälsa', gender: 'unisex' },
  'compression':    { sv: 'Återhämtning & Hälsa',  tag: 'kompression',   shopify: 'Återhämtning & Hälsa', gender: 'unisex' },
  'posture':        { sv: 'Återhämtning & Hälsa',  tag: 'hållning',      shopify: 'Återhämtning & Hälsa', gender: 'unisex' },
  'stretching':     { sv: 'Återhämtning & Hälsa',  tag: 'stretching',    shopify: 'Återhämtning & Hälsa', gender: 'unisex' },
  'sleep':          { sv: 'Återhämtning & Hälsa',  tag: 'sömn',          shopify: 'Återhämtning & Hälsa', gender: 'unisex' },
  'acupressure':    { sv: 'Återhämtning & Hälsa',  tag: 'återhämtning',  shopify: 'Återhämtning & Hälsa', gender: 'unisex' },
  'foam roller':    { sv: 'Återhämtning & Hälsa',  tag: 'foam roller',   shopify: 'Återhämtning & Hälsa', gender: 'unisex' },
  'health':         { sv: 'Återhämtning & Hälsa',  tag: 'hälsa',         shopify: 'Återhämtning & Hälsa', gender: 'unisex' },
  'smartwatch':     { sv: 'Smart Teknik',           tag: 'smartwatch',    shopify: 'Smart Teknik',          gender: 'unisex' },
  'gps watch':      { sv: 'Smart Teknik',           tag: 'gps',           shopify: 'Smart Teknik',          gender: 'unisex' },
  'tracker':        { sv: 'Smart Teknik',           tag: 'tracker',       shopify: 'Smart Teknik',          gender: 'unisex' },
  'earbuds':        { sv: 'Smart Teknik',           tag: 'hörlurar',      shopify: 'Smart Teknik',          gender: 'unisex' },
  'headphones':     { sv: 'Smart Teknik',           tag: 'hörlurar',      shopify: 'Smart Teknik',          gender: 'unisex' },
  'action camera':  { sv: 'Smart Teknik',           tag: 'kamera',        shopify: 'Smart Teknik',          gender: 'unisex' },
  'heart rate':     { sv: 'Smart Teknik',           tag: 'puls',          shopify: 'Smart Teknik',          gender: 'unisex' },
  'water bottle':   { sv: 'Kost & Vätska',          tag: 'vattenflaska',  shopify: 'Kost & Vätska',         gender: 'unisex' },
  'protein':        { sv: 'Kost & Vätska',          tag: 'protein',       shopify: 'Kost & Vätska',         gender: 'unisex' },
  'supplement':     { sv: 'Kost & Vätska',          tag: 'kosttillskott', shopify: 'Kost & Vätska',         gender: 'unisex' },
  'shaker':         { sv: 'Kost & Vätska',          tag: 'shaker',        shopify: 'Kost & Vätska',         gender: 'unisex' },
  'hydration':      { sv: 'Kost & Vätska',          tag: 'hydrering',     shopify: 'Kost & Vätska',         gender: 'unisex' },
  'electrolyte':    { sv: 'Kost & Vätska',          tag: 'elektrolyter',  shopify: 'Kost & Vätska',         gender: 'unisex' },
  'meal prep':      { sv: 'Kost & Vätska',          tag: 'meal prep',     shopify: 'Kost & Vätska',         gender: 'unisex' },
  'gym bag':        { sv: 'Utrustning & Tillbehör', tag: 'väska',         shopify: 'Utrustning & Tillbehör',gender: 'unisex' },
  'backpack':       { sv: 'Utrustning & Tillbehör', tag: 'ryggsäck',      shopify: 'Utrustning & Tillbehör',gender: 'unisex' },
  'gloves':         { sv: 'Utrustning & Tillbehör', tag: 'handskar',      shopify: 'Utrustning & Tillbehör',gender: 'unisex' },
  'belt':           { sv: 'Utrustning & Tillbehör', tag: 'bälte',         shopify: 'Utrustning & Tillbehör',gender: 'unisex' },
  'mat':            { sv: 'Utrustning & Tillbehör', tag: 'matta',         shopify: 'Utrustning & Tillbehör',gender: 'unisex' },
  'rope':           { sv: 'Utrustning & Tillbehör', tag: 'rep',           shopify: 'Utrustning & Tillbehör',gender: 'unisex' },
};

function mapCategory(rawCategory, productTitle) {
  const text = (rawCategory + ' ' + productTitle).toLowerCase();
  // STEG 1: Outdoor-first — dessa vinner alltid
  for (const kw of OUTDOOR_PRIORITY_KEYWORDS) {
    if (text.includes(kw)) {
      return { sv: 'Friluftsliv & Outdoor', tag: kw.split(' ')[0], shopify: 'Friluftsliv & Outdoor', gender: 'unisex' };
    }
  }
  // STEG 2: Normal mappning
  for (const [key, val] of Object.entries(CATEGORY_MAP)) {
    if (text.includes(key)) return val;
  }
  // STEG 3: Fallback
  return { sv: 'Träning & Fitness', tag: 'fitness', shopify: 'Träning & Fitness', gender: 'unisex' };
}

// ── COLLECTION HELPERS ─────────────────────────────────────
const shopifyCollections = {};
async function getOrCreateCollection(name) {
  if (shopifyCollections[name]) return shopifyCollections[name];
  const domain = process.env.SHOPIFY_DOMAIN;
  const token = process.env.SHOP_TOKEN || process.env.SHOPIFY_TOKEN;
  if (!domain || !token) return null;
  try {
    const res = await axios.get(
      `https://${domain}/admin/api/2024-01/custom_collections.json?title=${encodeURIComponent(name)}`,
      { headers: { 'X-Shopify-Access-Token': token } }
    );
    let col = res.data.custom_collections?.[0];
    if (!col) {
      const createRes = await axios.post(
        `https://${domain}/admin/api/2024-01/custom_collections.json`,
        { custom_collection: { title: name, published: true } },
        { headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' } }
      );
      col = createRes.data.custom_collection;
    }
    shopifyCollections[name] = col.id;
    return col.id;
  } catch(e) { console.error('Collection error:', e.message); return null; }
}

async function addProductToCollection(productId, collectionName) {
  const domain = process.env.SHOPIFY_DOMAIN;
  const token = process.env.SHOP_TOKEN || process.env.SHOPIFY_TOKEN;
  if (!domain || !token || !productId) return;
  try {
    const colId = await getOrCreateCollection(collectionName);
    if (!colId) return;
    await axios.post(
      `https://${domain}/admin/api/2024-01/collects.json`,
      { collect: { product_id: productId, collection_id: colId } },
      { headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' } }
    );
  } catch(e) { if (!e.message.includes('taken')) console.error('Collection add error:', e.message); }
}

// ── CONTENT FILTER ─────────────────────────────────────────
const BLOCKED_KEYWORDS = [
  'vibrator','dildo','buttplug','condom','penis','vagina','anal','fetish','bdsm','lingerie sexy',
  'gun','weapon','weapons','sword','knife','dagger','bullet','ammo','ammunition','firearm','pistol',
  'rifle','grenade','crossbow','tactical','military weapon',
  'drug','drugs','cannabis','marijuana','weed','cocaine','heroin','lsd',
  'whiskey','whisky','beer','wine','vodka','cigarette','tobacco','vape','e-cigarette','hookah',
  'gambling','casino','betting','poker','slot machine',
  'fake','replica','counterfeit','copy version','knockoff',
  'automobile','automotive','vehicle','motorcycle','speedometer','dashboard','windshield',
  'car mat clip','glovebox','engine part',
  'curtain','tablecloth','wall sticker','picture frame','vase','candle holder','home decor','home decoration',
  'pet dress','dog dress','cat costume','pet clothes','pet clothing',
  'beauty','skincare','skin care','facial','face patch','face patches','face mask','anti aging','anti-aging',
  'wrinkle','wrinkles','serum','cleanser','lip balm','lipstick','mascara',
  'foundation','eyeshadow','nail polish','eyelash','cosmetic','cosmetics','makeup','make-up','skin treatment',
  'jewelry','necklace','earring','bracelet','luxury watch','wedding dress',
  'evening dress','tuxedo','high heel','stiletto',
  'baby bottle','baby diaper','pacifier',
  'gaming keyboard','gaming mouse','led strip','phone case','tablet case'
];

const NICHE_KEYWORDS = [
  'gym','fitness','workout','training','exercise','crossfit','weightlifting','strength training',
  'resistance bands','pull up bar','ab roller','jump rope','gym gloves','weightlifting belt',
  'running','runner','running shoes','trail running','road running','running jacket','running shorts','hydration vest',
  'outdoor','camping','hiking','trekking','hiking boots','trekking poles','sleeping bag',
  'camping stove','camping tent','outdoor backpack','water filter',
  'cycling','cyclist','bike helmet','cycling jersey','cycling shorts','cycling gloves','cycling computer',
  'recovery','foam roller','massage gun','compression','compression sleeve','knee brace','ankle support','back stretcher','muscle recovery',
  'fitness tracker','heart rate monitor','gps watch','sports earbuds','running headphones','smartwatch sport',
  'bone-conduction','neckband headphones','bluetooth headphones','wireless headphones','open ear',
  'smartwatch','smart watch','smart ring','gps','action camera','cycling computer','bike computer',
  'heart rate strap','fitness watch','sport watch','running watch','triathlon watch',
  'bone conduction','wireless sports headphones','bluetooth sports earbuds','sports earbuds wireless',
  'headlamp','helmet camera','bike camera','cadence sensor','speed sensor','bike radar',
  'portable power station','power bank','solar charger','solar power bank',
  'training shorts','compression shirt','gym hoodie','running tights','athletic socks',
  'thermal base layer','sportswear','athletic wear',
  'träning','fitness','gym','löpning','vandring','friluftsliv','cykling','återhämtning','sportkläder','träningskläder'
];

function isProductBlocked(product) {
  const text = [product.title, product.nameEn, product.name, product.categoryName, product.description].join(' ').toLowerCase();
  for (const kw of BLOCKED_KEYWORDS) {
    if (text.includes(kw)) { console.log(`⛔ Blocked: "${product.title||product.nameEn}" — keyword: "${kw}"`); return true; }
  }
  const catText = (product.categoryName || '').toLowerCase();
  if (catText.includes('automobile') || catText.includes('car care') || catText.includes('vehicle') || catText.includes('motor')) {
    console.log(`⛔ Blocked auto category: "${product.title||product.nameEn}"`); return true;
  }
  const carBrands = ['ford ','toyota ','honda ','bmw ','mercedes ','audi ','volkswagen ','dodge ','pontiac ','chevrolet ','gmc ','cadillac ','jeep ','chrysler ','ram ','nissan ','mazda ','kia ','hyundai ','volvo ','saab ','peugeot ','renault '];
  for (const brand of carBrands) {
    if (text.includes(brand)) { console.log(`⛔ Blocked car brand: "${product.title||product.nameEn}"`); return true; }
  }
  const isNicheRelevant = NICHE_KEYWORDS.some(kw => text.includes(kw));
  if (!isNicheRelevant) { console.log(`⛔ Off-niche: "${product.title||product.nameEn}"`); return true; }
  return false;
}

// ── AUTO-PUBLISH THRESHOLD ─────────────────────────────────
const AUTO_PUBLISH_SCORE = 70; // >=70 → direkt till Meloni
const MIN_SCORE = 60;
const MAX_SHIPPING_DAYS = 15;

// ── ALIEXPRESS ─────────────────────────────────────────────
let aliExpressDisabled = false;

async function searchAliExpressProducts(keyword, limit = 20) {
  const rapidApiKey = process.env.RAPIDAPI_KEY;
  if (!rapidApiKey || aliExpressDisabled) { if (aliExpressDisabled) console.log('AliExpress disabled — monthly quota exceeded'); return []; }
  try {
    const res = await axios.get('https://aliexpress-datahub.p.rapidapi.com/item_search_2', {
      headers: { 'x-rapidapi-host': 'aliexpress-datahub.p.rapidapi.com', 'x-rapidapi-key': rapidApiKey },
      params: { q: keyword, page: '1', sort: 'SALE_PRICE_ASC' }
    });
    return res.data?.result?.resultList || [];
  } catch(e) {
    if (e.response?.status === 429 || e.message?.includes('MONTHLY') || e.message?.includes('quota')) {
      aliExpressDisabled = true; console.log('AliExpress monthly quota exceeded — disabling for this session');
    } else { console.error('AliExpress search failed:', e.message); }
    return [];
  }
}

async function getAliExpressProductDetail(itemId) {
  const rapidApiKey = process.env.RAPIDAPI_KEY;
  if (!rapidApiKey) return null;
  try {
    const res = await axios.get('https://aliexpress-datahub.p.rapidapi.com/item_detail_2', {
      headers: { 'x-rapidapi-host': 'aliexpress-datahub.p.rapidapi.com', 'x-rapidapi-key': rapidApiKey },
      params: { itemId: itemId.toString() }
    });
    return res.data?.result || null;
  } catch(e) { console.error('AliExpress detail failed:', e.message); return null; }
}

// ── CJ DROPSHIPPING ────────────────────────────────────────
let cjQuotaExhausted = false;

async function getCJToken() {
  if (process.env.CJ_API_KEY) {
    try {
      const res = await axios.post('https://developers.cjdropshipping.com/api2.0/v1/authentication/getAccessToken', { apiKey: process.env.CJ_API_KEY });
      if (res.data?.data?.accessToken) { console.log('✓ CJ token acquired via API key'); return res.data.data.accessToken; }
    } catch(e) { console.error('CJ API key auth error:', e.message); }
  }
  if (process.env.CJ_EMAIL && process.env.CJ_PASSWORD) {
    try {
      const res = await axios.post('https://developers.cjdropshipping.com/api2.0/v1/authentication/getAccessToken', { email: process.env.CJ_EMAIL, password: process.env.CJ_PASSWORD });
      if (res.data?.data?.accessToken) { console.log('✓ CJ token acquired via email/password'); return res.data.data.accessToken; }
    } catch(e) { console.error('CJ email auth error:', e.message); }
  }
  return null;
}

async function searchCJProducts(token, keyword, limit = 20) {
  if (cjQuotaExhausted) { console.log('CJ daily quota exhausted — skipping'); return []; }
  try {
    const res = await axios.get('https://developers.cjdropshipping.com/api2.0/v1/product/list', {
      headers: { 'CJ-Access-Token': token },
      params: { productNameEn: keyword, pageNum: 1, pageSize: limit, orderBy: 'ORDER_COUNT', orderType: 'DESC' }
    });
    const list = res.data?.data?.list || [];
    console.log(`CJ "${keyword}": ${list.length} results`);
    return list;
  } catch(e) {
    if (e.response?.data?.code === 1600200) { cjQuotaExhausted = true; console.log('⛔ CJ daily quota exhausted — stopping CJ requests for today'); return []; }
    console.error('CJ search error:', e.message); return [];
  }
}

function scoreCJProduct(product, index) {
  let score = 40;
  const text = [product.nameEn, product.name, product.categoryName, product.description].join(' ').toLowerCase();
  const price = parseFloat(product.sellPrice) || 10;
  if (price >= 5 && price <= 80) score += 20;
  const orders = parseInt(product.orderCount) || 0;
  score += Math.min(20, orders / 50);
  const imgs = (product.productImageSet || []).length;
  score += Math.min(10, imgs * 2);
  const premiumKeywords = ['running shoes','trail running','gym shoes','training shoes','hiking boots','outdoor backpack','gym bag','compression','massage gun','foam roller','cycling jersey','cycling shorts','fitness tracker','heart rate monitor','gps watch','smartwatch sport','sports earbuds','bone conduction headphones','action camera'];
  const goodKeywords = ['running','training','fitness','gym','hiking','trekking','camping','cycling','recovery','sportswear'];
  const weakKeywords = ['casual','fashion','lifestyle','daily wear','streetwear'];
  premiumKeywords.forEach(k => { if (text.includes(k)) score += 15; });
  goodKeywords.forEach(k => { if (text.includes(k)) score += 5; });
  weakKeywords.forEach(k => { if (text.includes(k)) score -= 10; });
  score += Math.max(0, 10 - index);
  return Math.max(0, Math.min(100, Math.round(score)));
}

// ── SIZE VARIANTS ──────────────────────────────────────────
function buildVariants(product) {
  const basePrice  = (product.sellPrice || 199).toString();
  const baseSku    = `VIN-${product.aliId || product.cjPid || 'AUTO'}`;
  const baseWeight = product.weight || 0.3;
  const baseStock  = product.stock  || 50;

  const CLOTHING_KW = ['shirt','tee','t-shirt','hoodie','jacket','vest','shorts','pants','leggings','tights','jersey','top','bra','sports bra','swimsuit','wetsuit','compression','tracksuit','sweatshirt','pullover','fleece','base layer','thermal','gloves','socks','beanie','hat','cap'];
  const SHOE_KW = ['shoes','boots','sneakers','trainers','runners','cleats','sandals','hiking boots','trail shoes','running shoes','gym shoes','cycling shoes'];
  const ONE_SIZE_KW = ['mat','roller','band','bottle','bag','backpack','watch','tracker','earbuds','headphones','camera','rope','kettle','weight','dumbbell','barbell','pole','tent','sleeping bag','stove','lamp','lantern'];

  const titleLower = (product.title || product.rawTitle || '').toLowerCase();
  const catLower   = (product.category || '').toLowerCase();
  const text       = titleLower + ' ' + catLower;

  if (product.cjVariants && Array.isArray(product.cjVariants) && product.cjVariants.length > 1) {
    return product.cjVariants.map((v, i) => ({
      option1: v.variantName || v.name || `Variant ${i + 1}`,
      price: basePrice, sku: `${baseSku}-${(v.variantName || i).toString().replace(/\s+/g, '-').toUpperCase()}`,
      inventory_management: 'shopify', inventory_quantity: v.variantStock || baseStock,
      weight: baseWeight, weight_unit: 'kg'
    }));
  }
  if (ONE_SIZE_KW.some(kw => text.includes(kw))) return [buildDefaultVariant(basePrice, baseSku, baseWeight, baseStock)];
  if (SHOE_KW.some(kw => text.includes(kw))) {
    return ['36','37','38','39','40','41','42','43','44','45'].map(size => ({
      option1: size, price: basePrice, sku: `${baseSku}-${size}`,
      inventory_management: 'shopify', inventory_quantity: baseStock, weight: baseWeight, weight_unit: 'kg'
    }));
  }
  if (text.includes('kids') || text.includes('children') || text.includes('barn')) {
    return ['90','100','110','120','130','140'].map(size => ({
      option1: size, price: basePrice, sku: `${baseSku}-${size}`,
      inventory_management: 'shopify', inventory_quantity: baseStock, weight: baseWeight, weight_unit: 'kg'
    }));
  }
  if (CLOTHING_KW.some(kw => text.includes(kw))) {
    return ['XS','S','M','L','XL','XXL'].map(size => ({
      option1: size, price: basePrice, sku: `${baseSku}-${size}`,
      inventory_management: 'shopify', inventory_quantity: baseStock, weight: baseWeight, weight_unit: 'kg'
    }));
  }
  return [buildDefaultVariant(basePrice, baseSku, baseWeight, baseStock)];
}

function buildDefaultVariant(price, sku, weight, stock) {
  return { price, sku, inventory_management: 'shopify', inventory_quantity: stock, weight, weight_unit: 'kg' };
}

// ── SHOPIFY PUBLISH ────────────────────────────────────────
async function publishToShopify(product) {
  const domain = process.env.SHOPIFY_DOMAIN;
  const token  = process.env.SHOP_TOKEN || process.env.SHOPIFY_TOKEN;
  if (!domain || !token) throw new Error('Shopify not configured');

  const variants = buildVariants(product);
  const payload = {
    product: {
      title: product.title,
      body_html: product.descriptionHtml,
      vendor: 'Meloni',
      product_type: product.category,
      tags: product.tags?.join(','),
      status: 'active',
      variants,
      options: variants.length > 1 && variants[0].option1
        ? [{ name: 'Storlek', values: variants.map(v => v.option1) }]
        : undefined,
      metafields: [
        { namespace: 'custom', key: 'shipping_days', value: String(product.shippingDays || 10), type: 'number_integer' },
        { namespace: 'custom', key: 'source', value: product.source || 'cj', type: 'single_line_text_field' }
      ],
      images: (product.images||[]).filter(u=>u&&u.startsWith('http')).slice(0,5).map((url,i) => ({ src: url, position: i+1 }))
    }
  };

  const res = await axios.post(
    `https://${domain}/admin/api/2024-01/products.json`,
    payload,
    { headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' } }
  );
  return res.data.product;
}

// ── EDIT LIVE PRODUCT ──────────────────────────────────────
// PATCH /api/products/:id — uppdatera titel, pris, bild, beskrivning direkt i Shopify
// GET  /api/products/:id/variants — hämta variant-IDs

// ── OPENAI CONTENT GENERATION ──────────────────────────────
async function generateProductContent(rawProduct) {
  const prompt = `Du är en erfaren svensk copywriter för Meloni — en modern, kurerad sportig livsstilsbutik. Du skriver som en riktig svensk människa, inte som en översättning från engelska. Naturlig, varm och trovärdig svenska.

Produkt: ${rawProduct.nameEn || rawProduct.name}
Kategori: ${rawProduct.categoryName || 'Allmänt'}
Beskrivning: ${rawProduct.description || 'Inte angiven'}

Skriv EXAKT i detta format (all text på svenska):

TITLE: [Kort, naturlig svensk titel — max 7 ord]

META: [SEO-beskrivning, max 155 tecken. Naturlig svenska, fördel-först. Inga utropstecken.]

DESCRIPTION: [2 korta stycken. Skriv avslappnat och trovärdigt. Ingen reklamsvenska.]

BENEFITS:
• [Konkret fördel — kort och tydlig, max 12 ord]
• [Konkret fördel — kort och tydlig, max 12 ord]
• [Konkret fördel — kort och tydlig, max 12 ord]

FAQ:
Fråga: [Den vanligaste frågan en svensk kund ställer om den här produkten]
Svar: [Kort, ärligt svar på 1-2 meningar.]
Fråga: [En fråga om leverans, storlek eller passform]
Svar: [Konkret svar — t.ex. "Finns i XS–XL. Leverans 7–14 dagar."]
Fråga: [En fråga om kvalitet, material eller hållbarhet]
Svar: [Ärligt svar som bygger förtroende utan att överdriva.]

VIKTIGT: Skriv ALLTID alla tre Fråga/Svar-par. Lämna aldrig ett Svar tomt.

AD_HOOK: [En rad för Instagram/TikTok — max 10 ord. Ska kännas äkta, inte som reklam.]

TAGS: [5 taggar, gemener, relevanta, på svenska]

Viktigt: Inga engelska ord om det finns ett bra svenskt alternativ. Inget "dropshipping", inga leverantörsnamn.`;

  const res = await openai.chat.completions.create({
    model: 'gpt-4o',
    max_tokens: 1200,
    messages: [
      { role: 'system', content: 'Du är en erfaren svensk copywriter specialiserad på aktiv livsstil, träning, friluftsliv och hälsa. Returnera endast det formaterade innehållet, inget annat.' },
      { role: 'user', content: prompt }
    ]
  });
  return res.choices[0].message.content;
}

function parseGeneratedContent(raw) {
  const get = (key) => {
    const pattern = new RegExp(`${key}:\\s*([\\s\\S]*?)(?=\\n[A-Z_]+:|$)`, 'i');
    const match = raw.match(pattern);
    return match ? match[1].trim() : '';
  };

  const title       = get('TITLE');
  const meta        = get('META');
  const description = get('DESCRIPTION');
  const benefits    = get('BENEFITS');
  const faq         = get('FAQ');
  const adHook      = get('AD_HOOK');
  const tagsRaw     = get('TAGS');

  // Render FAQ as proper HTML dl/dt/dd
  const faqHtml = (() => {
    const pairs = [];
    const lines = faq.split('\n').map(l => l.trim()).filter(Boolean);
    let currentQ = null;
    for (const line of lines) {
      if (line.toLowerCase().startsWith('fråga:') || line.toLowerCase().startsWith('q:')) {
        currentQ = line.replace(/^(fråga|q):\s*/i, '');
      } else if ((line.toLowerCase().startsWith('svar:') || line.toLowerCase().startsWith('a:')) && currentQ) {
        const answer = line.replace(/^(svar|a):\s*/i, '');
        pairs.push(`<dt><strong>${currentQ}</strong></dt><dd>${answer}</dd>`);
        currentQ = null;
      }
    }
    if (pairs.length === 0) return `<p>${faq}</p>`;
    return `<dl>${pairs.join('')}</dl>`;
  })();

  const descriptionHtml = `<p>${description.replace(/\n\n/g, '</p><p>')}</p>
<ul>${benefits.split('\n').filter(l => l.trim().startsWith('•')).map(l => `<li>${l.replace('•','').trim()}</li>`).join('')}</ul>
<h4>Vanliga frågor</h4>${faqHtml}`;

  return {
    title, meta, description, descriptionHtml,
    benefits: benefits.split('\n').filter(l => l.trim().startsWith('•')).map(l => l.replace('•','').trim()),
    adHook,
    tags: tagsRaw.split(',').map(t => t.trim()).filter(Boolean)
  };
}

// ── SCORING ────────────────────────────────────────────────
function scoreAliProduct(product, index) {
  let score = 0;
  const p = product.item || product;
  const price = parseFloat(p.sku?.def?.promotionPrice || p.sku?.def?.price || 5);
  if (price >= 2 && price <= 80) score += 20; else if (price > 0) score += 8;
  const sales = parseInt(p.sales || p.trade?.realTrade || 0);
  score += Math.min(30, sales / 50);
  const rating = parseFloat(p.averageStarRate || p.averageStar || 0);
  score += Math.round(rating * 4);
  if (p.image || p.imageUrl) score += 15;
  score += Math.max(0, 15 - index);
  return Math.min(100, Math.round(score));
}

function scoreProduct(product, index) {
  let score = 0;
  const costUsd = parseFloat(product.sellPrice) || 10;
  const sellPrice = costUsd * 5;
  const margin = ((sellPrice - costUsd) / sellPrice) * 100;
  score += Math.min(25, margin / 3);
  const orders = parseInt(product.orderCount) || 0;
  score += Math.min(20, orders / 100);
  const imgCount = product.productImageSet?.length || 0;
  score += Math.min(15, imgCount * 3);
  const shipDays = parseInt(product.shippingTime) || 20;
  score += Math.max(0, 15 - shipDays);
  if (product.variants?.length > 1) score += 10;
  score += Math.max(0, 15 - index);
  return Math.min(100, Math.round(score));
}

// ── TREND KEYWORDS ─────────────────────────────────────────
const TREND_KEYWORDS = [
  'men running jacket','men gym hoodie','men training shorts','men compression shirt','men hiking pants',
  'men running shoes','men trail running shoes','men gym shoes','men thermal base layer','men athletic socks',
  'women running jacket','women gym hoodie','women training shorts','women running tights','women compression leggings',
  'women running shoes','women trail running shoes','women gym shoes','women thermal base layer','women athletic socks',
  'resistance bands','pull up bar','ab roller','jump rope','gym gloves','weightlifting belt',
  'fitness tracker','heart rate monitor','gym bag','protein shaker',
  'running hydration vest','running belt','running cap','road running shoes','trail running shoes','running backpack',
  'hiking boots','trekking poles','camping tent','sleeping bag','outdoor backpack','waterproof jacket',
  'camping stove','water filter outdoor','camping lantern','camping cookware',
  'bike helmet','cycling gloves','cycling jersey','cycling shorts','cycling backpack','cycling computer',
  'massage gun','foam roller','knee brace support','back stretcher','compression sleeve','ankle support',
  'gps watch','running headphones','sports earbuds wireless','action camera','bone conduction headphones',
  'smartwatch','fitness watch','outdoor gps','headlamp','smart ring',
];

// ── MAIN RESEARCH PIPELINE ─────────────────────────────────
async function runProductResearch(overrideKeywords = null) {
  if (store.syncStatus === 'running') return;
  store.syncStatus = 'running';
  store.lastSync = new Date().toISOString();
  console.log(`[${new Date().toISOString()}] Starting product research...`);

  try {
    const shuffle = arr => [...arr].sort(() => Math.random() - 0.5);
    const delay = ms => new Promise(r => setTimeout(r, ms));
    const keywords = overrideKeywords ? overrideKeywords : shuffle(TREND_KEYWORDS).slice(0, 20);
    console.log('Researching keywords:', keywords);

    let candidates = [];

    if (process.env.RAPIDAPI_KEY) {
      console.log('Searching AliExpress...');
      for (const keyword of keywords) {
        const products = await searchAliExpressProducts(keyword, 10);
        store.stats.totalScanned += products.length;
        products.forEach((p, i) => {
          const score = scoreAliProduct(p, i);
          if (score >= 40) { const item = p.item || p; candidates.push({ ...item, score, keyword, source: 'aliexpress', _raw: p }); }
        });
      }
    }

    if ((process.env.CJ_EMAIL && process.env.CJ_PASSWORD) || process.env.CJ_API_KEY) {
      if (!cjQuotaExhausted) {
        console.log('Searching CJ Dropshipping...');
        const cjToken = await getCJToken();
        if (cjToken) {
          const cjKeywords = overrideKeywords ? overrideKeywords : shuffle(TREND_KEYWORDS).slice(0, 20);
          for (const keyword of cjKeywords) {
            if (cjQuotaExhausted) break;
            await delay(5000);
            const products = await searchCJProducts(cjToken, keyword, 10);
            store.stats.totalScanned += products.length;
            products.forEach((p, i) => {
              const score = scoreCJProduct(p, i);
              if (score >= 20) {
                candidates.push({
                  ...p, score, keyword, source: 'cj',
                  title: p.nameEn || p.name || p.productNameEn || '',
                  itemId: p.pid,
                  image: p.productImage || (p.productImageSet || [])[0] || '',
                  images: p.productImage ? [p.productImage] : (p.productImageSet || []),
                  salePrice: p.sellPrice, costPrice: parseFloat(p.sellPrice) || 5,
                });
              }
            });
          }
        }
      } else {
        console.log('CJ quota exhausted — skipping CJ this run');
      }
    }

    if (candidates.length === 0) { console.log('No candidates found'); store.syncStatus = 'idle'; return; }

    candidates.sort((a, b) => b.score - a.score);
    const top = candidates.slice(0, 300);
    console.log(`Found ${candidates.length} candidates, processing top ${top.length}`);

    for (const product of top) {
      if (isProductBlocked(product)) { store.stats.totalRejected++; continue; }

      const idExists = [...store.queue, ...store.products].find(p => p.aliId === String(product.itemId||product.productId||product.pid));
      if (idExists) { console.log(`⛔ Duplicate ID: ${product.title || product.nameEn}`); continue; }

      const productTitle = (product.title||product.nameEn||'').toLowerCase().slice(0,30);
      const titleExists = productTitle.length > 5 && [...store.queue, ...store.products].find(p => p.rawTitle?.toLowerCase().slice(0,30) === productTitle);
      if (titleExists) { console.log(`⛔ Duplicate Title: ${product.title || product.nameEn}`); continue; }

      try {
        const fixUrl = u => u ? (u.startsWith('//') ? 'https:' + u : u) : null;
        const images = [];
        const isCJProduct = product.source === 'cj';

        if (isCJProduct) {
          const primaryFallback = product.productImage || product.image || '';
          try {
            const cjToken = await getCJToken();
            if (cjToken && (product.pid || product.aliId)) {
              const detailRes = await axios.get('https://developers.cjdropshipping.com/api2.0/v1/product/query', {
                headers: { 'CJ-Access-Token': cjToken }, params: { pid: product.pid || product.aliId }
              });
              const detail = detailRes.data?.data;
              if (detail) {
                (detail.productImageSet || []).forEach(img => { if (img && !images.includes(img)) images.push(img); });
                (detail.variants || detail.productVariants || []).forEach(v => { if (v.variantImage && !images.includes(v.variantImage)) images.push(v.variantImage); });
              }
            }
          } catch(e) {}
          const cjImgs = product.images || product.productImageSet || [];
          cjImgs.slice(1).forEach(img => { if(img && !images.includes(img)) images.push(img); });
          if (images.length === 0 && primaryFallback) images.push(primaryFallback);
          else if (images.length < 3 && cjImgs[0] && !images.includes(cjImgs[0])) images.push(cjImgs[0]);
        } else {
          const imgBase = fixUrl(product.image || product.imageUrl || '');
          if (imgBase) images.push(imgBase);
          try {
            const detail = await getAliExpressProductDetail(product.itemId||product.productId);
            if (detail?.imageUrl) { const dImg = fixUrl(detail.imageUrl); if (dImg && !images.includes(dImg)) images.push(dImg); }
          } catch(e) {}
        }

        const costUsd = Math.max(2, parseFloat(product.sku?.def?.promotionPrice || product.sku?.def?.price || product.price?.minPrice || product.salePrice || 5));
        const rawSek = Math.round(costUsd * 5 * 9.5);
        const sellSek = snapPrice(rawSek);

        if (product.score < MIN_SCORE) { console.log(`⛔ Low score: "${product.title || product.nameEn}" (${product.score})`); store.stats.totalRejected++; continue; }

        const productName = product.title || product.nameEn || product.name || product.subject || product.productNameEn || 'Trending Product';
        const rawContent = await generateProductContent({
          nameEn: productName,
          description: product.productNameEn || product.nameEn || product.name || productName,
          sellPrice: costUsd,
          categoryName: product.productType || product.categoryName || product.category || 'General'
        });
        const content = parseGeneratedContent(rawContent);

        const queueItem = {
          id: `q_${Date.now()}_${Math.random().toString(36).slice(2,7)}`,
          aliId: String(product.itemId||product.productId||Math.random()),
          score: product.score, keyword: product.keyword,
          costPrice: costUsd, sellPrice: sellSek,
          margin: Math.round(((sellSek - costUsd*9.5) / sellSek) * 100),
          category: product.productType || 'General',
          stock: 99, shippingDays: 14,
          images: images.slice(0,5),
          title: content.title || product.title,
          meta: content.meta, description: content.description,
          descriptionHtml: content.descriptionHtml, benefits: content.benefits,
          adHook: content.adHook,
          tags: [...(content.tags||[]), mapCategory(product.categoryName || product.productType || '', product.title || product.nameEn || '').tag],
          rawTitle: product.title || product.name || product.subject || 'Product',
          aliUrl: `https://www.aliexpress.com/item/${product.itemId||product.productId}.html`,
          addedAt: new Date().toISOString(), status: 'pending',
          autoPublish: product.score >= AUTO_PUBLISH_SCORE
        };

        if (queueItem.autoPublish) {
          console.log(`🚀 Auto-publishing: ${content.title} (score: ${product.score})`);
          try {
            const shopifyProduct = await publishToShopify(queueItem);
            queueItem.status = 'approved';
            queueItem.shopifyId = shopifyProduct.id;
            if (registerNewProduct && db) registerNewProduct(shopifyProduct.id, shopifyProduct.title, db).catch(()=>{});
            queueItem.approvedAt = new Date().toISOString();
            store.products.push(queueItem);
            store.stats.totalApproved++;
            await dbSave('products', queueItem);
            try { const { assignCollections } = require('./collectionAssign'); assignCollections(shopifyProduct.id, queueItem.title, queueItem.tags, db).catch(()=>{}); } catch(e) {}
            console.log(`✓ Auto-published: ${content.title} → Shopify ID ${shopifyProduct.id}`);
            triggerMakeWebhook('product_approved', { title: queueItem.title, price: queueItem.sellPrice, shopifyId: shopifyProduct.id });
            sendEmailNotification(`🚀 Auto-published: ${queueItem.title}`, `<h2>${queueItem.title}</h2><p>Score: ${queueItem.score}/100</p>`);
            continue;
          } catch(publishErr) {
            console.error('Auto-publish failed, adding to queue instead:', publishErr.message);
            queueItem.autoPublish = false;
          }
        }

        store.queue.push(queueItem);
        store.stats.totalQueued++;
        await dbSave('queue', queueItem);
        console.log(`✓ Queued: ${content.title} (score: ${product.score})`);
        triggerMakeWebhook('product_queued', { title: queueItem.title, score: queueItem.score });
        sendEmailNotification(`🔍 New product ready for review: ${queueItem.title}`, `<h2>${queueItem.title}</h2><p>Score: ${queueItem.score}/100</p>`);

      } catch(contentErr) {
        console.error('Content generation failed:', contentErr.message);
      }
    }

    store.syncStatus = 'idle';
    console.log(`Research complete. Queue: ${store.queue.length}`);
  } catch(err) {
    console.error('Research pipeline error:', err.message);
    store.syncStatus = 'error';
  }
}

// ── ORBIT ─────────────────────────────────────────────────
async function triggerOrbitCampaign(product) {
  const orbitUrl = process.env.ORBIT_API_URL;
  const orbitToken = process.env.ORBIT_API_TOKEN;
  const orbitBrand = process.env.ORBIT_BRAND_ID;
  if (!orbitUrl || !orbitToken || !orbitBrand) return null;
  try {
    const campaignRes = await axios.post(`${orbitUrl}/api/v1/campaigns`, {
      brand_id: orbitBrand, name: `${product.title} — Launch`, type: 'product_launch', status: 'draft',
      product: { title: product.title, description: product.description, price: `${product.sellPrice} kr`, category: product.category, tags: product.tags, images: product.images?.slice(0,5), ad_hook: product.adHook, benefits: product.benefits, shopify_id: product.shopifyId, meta_description: product.meta },
      goals: { objective: 'product_awareness', channels: ['instagram','tiktok','meta_ads'] }
    }, { headers: { 'Authorization': `Bearer ${orbitToken}`, 'Content-Type': 'application/json' } });
    return campaignRes.data;
  } catch(err) { console.error('Orbit trigger failed:', err.message); return null; }
}

// ── MAKE.COM ──────────────────────────────────────────────
async function triggerMakeWebhook(event, data) {
  const webhookUrl = process.env.MAKE_WEBHOOK_URL;
  if (!webhookUrl) return null;
  try {
    await axios.post(webhookUrl, { event, timestamp: new Date().toISOString(), store: process.env.SHOPIFY_DOMAIN, data }, { headers: { 'Content-Type': 'application/json' } });
  } catch(err) { console.error('Make webhook failed:', err.message); }
}

// ── EMAIL ─────────────────────────────────────────────────
async function sendEmailNotification(subject, html) {
  const resendKey = process.env.RESEND_API_KEY;
  const notifyEmail = process.env.NOTIFY_EMAIL;
  if (!resendKey || !notifyEmail) return null;
  try {
    await axios.post('https://api.resend.com/emails', {
      from: 'Mercury AI <mercury@meloni.se>', to: notifyEmail, subject, html
    }, { headers: { 'Authorization': `Bearer ${resendKey}`, 'Content-Type': 'application/json' } });
  } catch(err) { console.error('Email failed:', err.message); }
}

// ── SHOPIFY PERFORMANCE ────────────────────────────────────
async function syncShopifyPerformance() {
  const domain = process.env.SHOPIFY_DOMAIN;
  const token  = process.env.SHOP_TOKEN || process.env.SHOPIFY_TOKEN;
  if (!domain || !token) return;
  try {
    const since = new Date(Date.now() - 30*24*60*60*1000).toISOString();
    const ordersRes = await axios.get(`https://${domain}/admin/api/2024-01/orders.json?status=any&created_at_min=${since}&limit=250`, { headers: { 'X-Shopify-Access-Token': token } });
    const orders = ordersRes.data.orders || [];
    let totalRevenue = 0;
    const productSales = {};
    orders.forEach(order => {
      if (order.financial_status === 'paid' || order.financial_status === 'partially_paid') {
        totalRevenue += parseFloat(order.total_price || 0);
        order.line_items?.forEach(item => {
          const pid = item.product_id?.toString();
          if (!productSales[pid]) productSales[pid] = { units: 0, revenue: 0, title: item.title };
          productSales[pid].units += item.quantity;
          productSales[pid].revenue += parseFloat(item.price) * item.quantity;
        });
      }
    });
    store.products.forEach(p => {
      const shopifyId = p.shopifyId?.toString();
      if (shopifyId && productSales[shopifyId]) { p.unitsSold = productSales[shopifyId].units; p.revenue30d = Math.round(productSales[shopifyId].revenue); }
    });
    store.performance = { revenue30d: Math.round(totalRevenue), orderCount: orders.filter(o => o.financial_status === 'paid').length, lastUpdated: new Date().toISOString() };
    console.log(`✓ Performance synced`);
  } catch(err) { console.error('Performance sync failed:', err.message); }
}

// ── API ROUTES ─────────────────────────────────────────────

app.get('/api/auth', (req, res) => {
  const apiKey = process.env.SHOPIFY_API_KEY;
  const scopes = 'read_products,write_products,read_orders,write_orders,read_inventory,write_inventory';
  const redirectUri = `https://mercury-production-ace6.up.railway.app/api/auth/callback`;
  const shop = req.query.shop || process.env.SHOPIFY_DOMAIN;
  res.redirect(`https://${shop}/admin/oauth/authorize?client_id=${apiKey}&scope=${scopes}&redirect_uri=${encodeURIComponent(redirectUri)}`);
});

app.get('/api/auth/callback', async (req, res) => {
  const { code, shop } = req.query;
  try {
    const tokenRes = await axios.post(`https://${shop}/admin/oauth/access_token`, { client_id: process.env.SHOPIFY_API_KEY, client_secret: process.env.SHOPIFY_API_SECRET, code });
    const accessToken = tokenRes.data.access_token;
    process.env.SHOP_TOKEN = accessToken;
    process.env.SHOPIFY_TOKEN = accessToken;
    res.send(`<h2>✓ Connected!</h2><p>Token: <code>${accessToken}</code></p>`);
  } catch(e) { res.status(500).send('OAuth failed: ' + e.message); }
});

app.get('/api/status', (req, res) => {
  res.json({ ok: true, syncStatus: store.syncStatus, lastSync: store.lastSync, queueCount: store.queue.length, productCount: store.products.length, stats: store.stats });
});

app.get('/api/queue', (req, res) => { res.json(store.queue.filter(p => p.status === 'pending')); });
app.get('/api/products', (req, res) => { res.json(store.products); });

// ── REDIGERA LIVE-PRODUKT ──────────────────────────────────
app.patch('/api/products/:id', async (req, res) => {
  const item = store.products.find(p => p.id === req.params.id || p.shopifyId == req.params.id);
  if (!item) return res.status(404).json({ error: 'Produkten hittades inte' });
  if (!item.shopifyId) return res.status(400).json({ error: 'Ingen Shopify-koppling' });

  const domain = process.env.SHOPIFY_DOMAIN;
  const token  = process.env.SHOP_TOKEN || process.env.SHOPIFY_TOKEN;
  if (!domain || !token) return res.status(500).json({ error: 'Shopify ej konfigurerat' });

  const { title, sellPrice, description, primaryImage, images } = req.body;
  const updates = {};
  if (title)       updates.title     = title;
  if (description) updates.body_html = `<p>${description.replace(/\n\n/g, '</p><p>')}</p>`;
  if (sellPrice)   updates.variants  = [{ id: item.shopifyVariantId, price: String(sellPrice) }];
  if (primaryImage || images) {
    const allImages = images || item.images || [];
    const ordered = primaryImage ? [primaryImage, ...allImages.filter(u => u !== primaryImage)] : allImages;
    updates.images = ordered.filter(u => u && u.startsWith('http')).slice(0,5).map((url,i) => ({ src: url, position: i+1 }));
  }
  if (Object.keys(updates).length === 0) return res.status(400).json({ error: 'Inga fält att uppdatera' });

  try {
    const shopifyRes = await axios.put(`https://${domain}/admin/api/2024-01/products/${item.shopifyId}.json`, { product: updates }, { headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' } });
    if (title)        item.title       = title;
    if (sellPrice)    item.sellPrice   = sellPrice;
    if (description)  item.description = description;
    if (primaryImage) item.images = [primaryImage, ...(item.images||[]).filter(u => u !== primaryImage)];
    if (images)       item.images = images;
    await dbSave('products', item);
    res.json({ ok: true, shopifyProduct: shopifyRes.data.product, local: item });
  } catch(err) { res.status(500).json({ error: err.response?.data?.errors || err.message }); }
});

app.get('/api/products/:id/variants', async (req, res) => {
  const item = store.products.find(p => p.id === req.params.id || p.shopifyId == req.params.id);
  if (!item?.shopifyId) return res.status(404).json({ error: 'Produkt eller Shopify-koppling saknas' });
  const domain = process.env.SHOPIFY_DOMAIN;
  const token  = process.env.SHOP_TOKEN || process.env.SHOPIFY_TOKEN;
  try {
    const r = await axios.get(`https://${domain}/admin/api/2024-01/products/${item.shopifyId}/variants.json`, { headers: { 'X-Shopify-Access-Token': token } });
    res.json({ variants: r.data.variants });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/approve/:id', async (req, res) => {
  const item = store.queue.find(p => p.id === req.params.id);
  if (!item) return res.status(404).json({ error: 'Not found' });
  try {
    if (req.body.title)       item.title       = req.body.title;
    if (req.body.description) item.description = req.body.description;
    if (req.body.sellPrice)   item.sellPrice   = req.body.sellPrice;
    const shopifyProduct = await publishToShopify(item);
    item.status = 'approved';
    item.shopifyId = shopifyProduct.id;
    if (registerNewProduct && db) registerNewProduct(shopifyProduct.id, shopifyProduct.title, db).catch(e => console.error('Score register failed:', e.message));
    item.approvedAt = new Date().toISOString();
    store.products.push(item);
    store.queue = store.queue.filter(p => p.id !== req.params.id);
    store.stats.totalApproved++;
    await dbSave('products', item);
    await dbDelete('queue', item.id);
    try { const { assignCollections } = require('./collectionAssign'); assignCollections(shopifyProduct.id, item.title, item.tags, db).catch(e => console.error('Collection assign error:', e.message)); } catch(e) {}
    Promise.all([
      triggerOrbitCampaign(item),
      triggerMakeWebhook('product_approved', { title: item.title, price: item.sellPrice, shopifyId: item.shopifyId }),
      sendEmailNotification(`✓ New product live: ${item.title}`, `<h2>${item.title}</h2><p>Score: ${item.score}/100 | Price: ${item.sellPrice} kr</p>`)
    ]).catch(err => console.error('Integration error:', err.message));
    res.json({ ok: true, shopifyId: shopifyProduct.id, product: item });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/reject/:id', async (req, res) => {
  const item = store.queue.find(p => p.id === req.params.id);
  if (!item) return res.status(404).json({ error: 'Not found' });
  item.status = 'rejected';
  store.removed.push(item);
  store.queue = store.queue.filter(p => p.id !== req.params.id);
  store.stats.totalRejected++;
  await dbDelete('queue', item.id);
  await dbSave('removed', item);
  res.json({ ok: true });
});

app.post('/api/regenerate/:id', async (req, res) => {
  const item = store.queue.find(p => p.id === req.params.id);
  if (!item) return res.status(404).json({ error: 'Not found' });
  try {
    const rawContent = await generateProductContent({ nameEn: item.rawTitle, description: item.description, sellPrice: item.costPrice });
    const content = parseGeneratedContent(rawContent);
    item.title = content.title; item.description = content.description;
    item.descriptionHtml = content.descriptionHtml; item.benefits = content.benefits;
    item.adHook = content.adHook; item.tags = content.tags;
    res.json({ ok: true, item });
  } catch(err) { res.status(500).json({ error: err.message }); }
});

// ── SEED & RESEARCH ENDPOINTS ──────────────────────────────
app.post('/api/research/run', async (req, res) => {
  res.json({ ok: true, message: 'Research started' });
  runProductResearch();
});

app.post('/api/research/keyword', async (req, res) => {
  const { keyword } = req.body;
  if (!keyword) return res.status(400).json({ error: 'keyword required' });
  res.json({ ok: true, message: `Researching: ${keyword}` });
  runProductResearch([keyword]);
});

app.post('/api/seed', async (req, res) => {
  const { categories, delay_ms = 8000 } = req.body || {};

  const SEED_KEYWORDS = {
    'herr-tshirts':         ['men sport t-shirt gym', 'men tank top training', 'men gym shirt dry fit'],
    'herr-hoodies':         ['men hoodie sport', 'men sweatshirt training', 'men pullover fleece sport'],
    'herr-funktion':        ['men compression shirt long sleeve', 'men base layer top', 'men running shirt lightweight'],
    'herr-kompression':     ['men compression tights', 'men compression leggings sport', 'men compression pants running'],
    'herr-shorts':          ['men gym shorts', 'men running shorts', 'men training shorts 2in1'],
    'herr-byxor':           ['men joggers sport', 'men track pants training', 'men sweatpants tapered'],
    'herr-understall':      ['men thermal underwear set', 'men base layer winter', 'men long johns sport'],
    'herr-jackor':          ['men windbreaker running jacket', 'men softshell jacket outdoor', 'men fleece jacket sport'],
    'herr-strumpor':        ['men running socks', 'men compression socks sport', 'men athletic socks ankle'],
    'herr-traningsskor':    ['men training shoes crossfit', 'men gym shoes', 'men cross trainer shoes'],
    'herr-loparskor':       ['men running shoes lightweight', 'men jogging shoes road'],
    'herr-trailskor':       ['men trail running shoes', 'men trail shoes grip'],
    'herr-vandring':        ['men hiking boots waterproof', 'men trekking boots ankle'],
    'herr-sandaler':        ['men sport sandals outdoor', 'men trekking sandals'],
    'dam-tshirts':          ['women sport t-shirt', 'women tank top gym', 'women workout top dry fit'],
    'dam-hoodies':          ['women hoodie sport', 'women sweatshirt zip', 'women pullover gym'],
    'dam-funktion':         ['women base layer top', 'women compression shirt sport', 'women long sleeve running'],
    'dam-kompression':      ['women compression tights yoga', 'women sport leggings high waist', 'women yoga pants seamless'],
    'dam-shorts':           ['women gym shorts', 'women biker shorts', 'women running shorts'],
    'dam-byxor':            ['women joggers sport', 'women training pants', 'women track pants'],
    'dam-jackor':           ['women windbreaker jacket sport', 'women fleece jacket', 'women rain jacket running'],
    'dam-understall':       ['women thermal underwear', 'women base layer winter sport'],
    'sport-bh-latt':        ['sports bra low impact yoga', 'light support bra workout'],
    'sport-bh-medium':      ['sports bra medium support', 'medium impact sports bra training'],
    'sport-bh-hog':         ['sports bra high impact running', 'high support sports bra'],
    'dam-loparskor':        ['women running shoes', 'women jogging shoes lightweight'],
    'dam-traningsskor':     ['women training shoes gym', 'women crossfit shoes'],
    'dam-vandring':         ['women hiking boots waterproof', 'women trekking shoes'],
    'barn-tshirts':         ['kids sport t-shirt', 'children hoodie sport', 'youth gym shirt'],
    'barn-shorts':          ['kids sport shorts', 'children training pants', 'youth gym shorts'],
    'barn-jackor':          ['kids rain jacket outdoor', 'children windbreaker jacket'],
    'barn-outdoor':         ['kids outdoor clothing set', 'children hiking pants', 'youth fleece jacket'],
    'barn-understall':      ['kids thermal underwear', 'children base layer sport'],
    'barn-traningsskor':    ['kids training shoes', 'children gym shoes sport'],
    'barn-loparskor':       ['kids running shoes', 'children sport shoes running'],
    'barn-outdoorskor':     ['kids outdoor shoes', 'children hiking shoes'],
    'barn-vandring':        ['kids hiking boots', 'children trekking boots waterproof'],
    'fitness-styrka':       ['resistance bands set loop', 'pull up bar doorway', 'ab roller wheel core', 'push up handles'],
    'fitness-kondition':    ['jump rope speed crossfit', 'battle rope training', 'agility ladder sport'],
    'fitness-utrustning':   ['weightlifting belt support', 'gym gloves grip', 'wrist wraps lifting', 'lifting straps gym'],
    'fitness-tillbehor':    ['gym bag men', 'gym bag women', 'gym towel microfiber', 'sport water bottle gym'],
    'outdoor-talt':         ['camping tent 2 person lightweight', 'backpacking tent ultralight'],
    'outdoor-sovs':         ['sleeping bag 3 season', 'sleeping bag mummy lightweight'],
    'outdoor-ryggsack':     ['hiking backpack 30l', 'trekking backpack daypack', 'outdoor backpack waterproof'],
    'outdoor-nav':          ['trekking poles carbon', 'hiking poles adjustable', 'headlamp rechargeable camping'],
    'outdoor-kok':          ['camping stove portable', 'camping cookware set titanium', 'water filter hiking'],
    'outdoor-safety':       ['carabiner climbing', 'survival kit outdoor', 'emergency blanket camping'],
    'recovery-massage':     ['massage gun deep tissue', 'foam roller muscle recovery', 'massage ball trigger point'],
    'recovery-stod':        ['knee brace support sport', 'ankle brace support', 'elbow brace tennis'],
    'recovery-stretch':     ['stretching strap flexibility', 'posture corrector back', 'acupressure mat neck'],
    'teknik-klockor':       ['fitness tracker watch sport', 'gps running watch', 'heart rate monitor chest'],
    'teknik-ljud':          ['wireless earbuds sport waterproof', 'bone conduction headphones running'],
    'teknik-gadgets':       ['action camera sport waterproof', 'cycling computer gps', 'phone arm band running'],
    'kost-flaskor':         ['insulated water bottle 1l', 'protein shaker bottle', 'hydration vest running'],
    'kost-nutrition':       ['electrolyte powder sport', 'energy gel running', 'meal prep container set'],
    'lopning-accessoarer':  ['running belt waist', 'running vest reflective', 'running headband', 'arm band phone running'],
    'vandring-accessoarer': ['trekking poles ultralight', 'sleeping pad camping', 'camp lantern led', 'hiking water filter'],
    'cykling-klader':       ['cycling jersey men short sleeve', 'cycling shorts bib padded', 'cycling jacket windproof'],
    'cykling-accessoarer':  ['cycling gloves gel padded', 'bike light front rear set', 'bike bag frame', 'cycling helmet'],
    'yoga-utrustning':      ['yoga mat non slip thick', 'yoga block cork set', 'yoga strap stretch', 'yoga wheel back'],
    'yoga-klader':          ['yoga leggings women seamless', 'pilates socks grip', 'meditation cushion'],
    'livsstil':             ['active lifestyle bag', 'wellness sport accessories', 'sport lifestyle clothing'],
  };

  const selected = categories
    ? Object.entries(SEED_KEYWORDS).filter(([k]) => categories.includes(k))
    : Object.entries(SEED_KEYWORDS);

  const total = selected.reduce((n, [, kws]) => n + kws.length, 0);
  res.json({ ok: true, message: `Seeding ${total} keywords across ${selected.length} categories. Takes ~${Math.round(total * delay_ms / 60000)} min.`, total });

  (async () => {
    const delay = ms => new Promise(r => setTimeout(r, ms));
    for (const [cat, keywords] of selected) {
      console.log(`[SEED] Category: ${cat}`);
      for (const kw of keywords) {
        console.log(`[SEED] → ${kw}`);
        await runProductResearch([kw]);
        await delay(delay_ms);
      }
    }
    console.log('[SEED] ✓ Complete');
  })();
});

app.get('/api/performance', (req, res) => {
  res.json({ performance: store.performance || null, products: store.products.map(p => ({ id: p.id, title: p.title, shopifyId: p.shopifyId, unitsSold: p.unitsSold || 0, revenue30d: p.revenue30d || 0, sellPrice: p.sellPrice, score: p.score })) });
});

app.post('/api/performance/sync', async (req, res) => {
  res.json({ ok: true, message: 'Syncing...' });
  syncShopifyPerformance();
});

app.get('/api/dashboard', (req, res) => {
  res.json({ syncStatus: store.syncStatus, lastSync: store.lastSync, queueCount: store.queue.length, productCount: store.products.length, stats: store.stats, performance: store.performance || null, recentQueue: store.queue.slice(-3), topProducts: [...store.products].sort((a,b)=>(b.revenue30d||0)-(a.revenue30d||0)).slice(0,5) });
});

app.get('/api/create-collections', async (req, res) => {
  const domain = process.env.SHOPIFY_DOMAIN;
  const token  = process.env.SHOP_TOKEN || process.env.SHOPIFY_TOKEN;
  if (!domain || !token) return res.status(500).json({ error: 'Shopify not configured' });
  const COLLS = [
    { title: 'Nyheter', handle: 'nyheter' }, { title: 'Bästsäljare', handle: 'bestsellers' }, { title: 'Rea', handle: 'rea' },
    { title: 'Herr', handle: 'herr' }, { title: 'Dam', handle: 'dam' }, { title: 'Barn', handle: 'barn' },
    { title: 'Träning & Fitness', handle: 'traning-fitness' }, { title: 'Löpning', handle: 'lopning' },
    { title: 'Outdoor & Camping', handle: 'outdoor-camping' }, { title: 'Vandring', handle: 'vandring' },
    { title: 'Cykling', handle: 'cykling' }, { title: 'Yoga & Pilates', handle: 'yoga-pilates' },
    { title: 'Smart Tech', handle: 'smart-tech' }, { title: 'Återhämtning & Hälsa', handle: 'recovery-health' },
    { title: 'Kost & Hydrering', handle: 'kost-hydrering' }, { title: 'Utrustning & Tillbehör', handle: 'utrustning-tillbehor' },
  ];
  const results = { created: [], existing: [], failed: [] };
  const delay = ms => new Promise(r => setTimeout(r, ms));
  for (const col of COLLS) {
    try {
      const checkRes = await axios.get(`https://${domain}/admin/api/2024-01/custom_collections.json?handle=${col.handle}`, { headers: { 'X-Shopify-Access-Token': token } });
      if (checkRes.data.custom_collections?.length > 0) { results.existing.push(col.title); }
      else {
        await axios.post(`https://${domain}/admin/api/2024-01/custom_collections.json`, { custom_collection: { title: col.title, handle: col.handle, published: true } }, { headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' } });
        results.created.push(col.title);
      }
      await delay(300);
    } catch(e) { results.failed.push({ title: col.title, error: e.message }); }
  }
  res.json({ ok: true, created: results.created.length, existing: results.existing.length, failed: results.failed.length, details: results });
});

// ── CRON ──────────────────────────────────────────────────
cron.schedule('0 */12 * * *', () => { console.log('Cron: Starting scheduled research...'); runProductResearch(); });
cron.schedule('0 * * * *', () => { console.log('Cron: Syncing performance...'); syncShopifyPerformance(); });

// ── START ──────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
app.listen(PORT, async () => {
  await initDB();
  if (trackingRoutes && db) { app.use('/track', trackingRoutes(db)); console.log('✓ Tracking routes mounted'); }
  console.log(`Mercury Backend running on port ${PORT}`);
  console.log('Shopify:', process.env.SHOPIFY_DOMAIN || 'NOT SET');
  console.log('OpenAI:', process.env.OPENAI_API_KEY ? 'SET' : 'NOT SET');
  console.log('CJ:', process.env.CJ_API_KEY ? 'API Key SET' : (process.env.CJ_EMAIL || 'NOT SET'));
  setTimeout(syncShopifyPerformance, 10000);
  setTimeout(runProductResearch, 5000);
});