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
// Env aliases for collectionAssign compatibility
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
    // Load existing data into memory
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

// ── IN-MEMORY STORE (replace with DB later) ────────────────
let store = {
  products: [],        // approved live products
  queue: [],           // pending approval
  removed: [],         // rejected/removed
  lastSync: null,
  syncStatus: 'idle',  // idle | running | error
  stats: {
    totalScanned: 0,
    totalQueued: 0,
    totalApproved: 0,
    totalRejected: 0,
  }
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


// ── CATEGORY MAPPING (Fitness/Outdoor/Health niche) ──────────
const CATEGORY_MAP = {
  // Träning & Fitness
  'gym': { sv: 'Träning & Fitness', tag: 'gym', shopify: 'Träning & Fitness', gender: 'unisex' },
  'fitness': { sv: 'Träning & Fitness', tag: 'fitness', shopify: 'Träning & Fitness', gender: 'unisex' },
  'workout': { sv: 'Träning & Fitness', tag: 'träning', shopify: 'Träning & Fitness', gender: 'unisex' },
  'training': { sv: 'Träning & Fitness', tag: 'träning', shopify: 'Träning & Fitness', gender: 'unisex' },
  'resistance': { sv: 'Träning & Fitness', tag: 'styrketräning', shopify: 'Träning & Fitness', gender: 'unisex' },
  'yoga': { sv: 'Träning & Fitness', tag: 'yoga', shopify: 'Träning & Fitness', gender: 'dam' },
  'crossfit': { sv: 'Träning & Fitness', tag: 'crossfit', shopify: 'Träning & Fitness', gender: 'unisex' },
  'weightlifting': { sv: 'Träning & Fitness', tag: 'styrketräning', shopify: 'Träning & Fitness', gender: 'herr' },
  'cardio': { sv: 'Träning & Fitness', tag: 'kondition', shopify: 'Träning & Fitness', gender: 'unisex' },

  // Friluftsliv & Outdoor
  'hiking': { sv: 'Friluftsliv & Outdoor', tag: 'vandring', shopify: 'Friluftsliv & Outdoor', gender: 'unisex' },
  'outdoor': { sv: 'Friluftsliv & Outdoor', tag: 'outdoor', shopify: 'Friluftsliv & Outdoor', gender: 'unisex' },
  'camping': { sv: 'Friluftsliv & Outdoor', tag: 'camping', shopify: 'Friluftsliv & Outdoor', gender: 'unisex' },
  'trekking': { sv: 'Friluftsliv & Outdoor', tag: 'vandring', shopify: 'Friluftsliv & Outdoor', gender: 'unisex' },
  'climbing': { sv: 'Friluftsliv & Outdoor', tag: 'klättring', shopify: 'Friluftsliv & Outdoor', gender: 'unisex' },
  'cycling': { sv: 'Friluftsliv & Outdoor', tag: 'cykling', shopify: 'Friluftsliv & Outdoor', gender: 'unisex' },
  'trail': { sv: 'Friluftsliv & Outdoor', tag: 'trail', shopify: 'Friluftsliv & Outdoor', gender: 'unisex' },
  'running': { sv: 'Friluftsliv & Outdoor', tag: 'löpning', shopify: 'Friluftsliv & Outdoor', gender: 'unisex' },

  // Återhämtning & Hälsa
  'recovery': { sv: 'Återhämtning & Hälsa', tag: 'återhämtning', shopify: 'Återhämtning & Hälsa', gender: 'unisex' },
  'massage': { sv: 'Återhämtning & Hälsa', tag: 'massage', shopify: 'Återhämtning & Hälsa', gender: 'unisex' },
  'brace': { sv: 'Återhämtning & Hälsa', tag: 'skydd', shopify: 'Återhämtning & Hälsa', gender: 'unisex' },
  'compression': { sv: 'Återhämtning & Hälsa', tag: 'kompression', shopify: 'Återhämtning & Hälsa', gender: 'unisex' },
  'posture': { sv: 'Återhämtning & Hälsa', tag: 'hållning', shopify: 'Återhämtning & Hälsa', gender: 'unisex' },
  'stretching': { sv: 'Återhämtning & Hälsa', tag: 'stretching', shopify: 'Återhämtning & Hälsa', gender: 'unisex' },
  'sleep': { sv: 'Återhämtning & Hälsa', tag: 'sömn', shopify: 'Återhämtning & Hälsa', gender: 'unisex' },
  'acupressure': { sv: 'Återhämtning & Hälsa', tag: 'återhämtning', shopify: 'Återhämtning & Hälsa', gender: 'unisex' },
  'foam roller': { sv: 'Återhämtning & Hälsa', tag: 'foam roller', shopify: 'Återhämtning & Hälsa', gender: 'unisex' },

  // Smart Teknik
  'smartwatch': { sv: 'Smart Teknik', tag: 'smartwatch', shopify: 'Smart Teknik', gender: 'unisex' },
  'gps watch': { sv: 'Smart Teknik', tag: 'gps', shopify: 'Smart Teknik', gender: 'unisex' },
  'tracker': { sv: 'Smart Teknik', tag: 'tracker', shopify: 'Smart Teknik', gender: 'unisex' },
  'earbuds': { sv: 'Smart Teknik', tag: 'hörlurar', shopify: 'Smart Teknik', gender: 'unisex' },
  'headphones': { sv: 'Smart Teknik', tag: 'hörlurar', shopify: 'Smart Teknik', gender: 'unisex' },
  'action camera': { sv: 'Smart Teknik', tag: 'kamera', shopify: 'Smart Teknik', gender: 'unisex' },
  'heart rate': { sv: 'Smart Teknik', tag: 'puls', shopify: 'Smart Teknik', gender: 'unisex' },
  'cycling computer': { sv: 'Smart Teknik', tag: 'cykling', shopify: 'Smart Teknik', gender: 'unisex' },

  // Kost & Vätska
  'water bottle': { sv: 'Kost & Vätska', tag: 'vattenflaska', shopify: 'Kost & Vätska', gender: 'unisex' },
  'protein': { sv: 'Kost & Vätska', tag: 'protein', shopify: 'Kost & Vätska', gender: 'unisex' },
  'supplement': { sv: 'Kost & Vätska', tag: 'kosttillskott', shopify: 'Kost & Vätska', gender: 'unisex' },
  'shaker': { sv: 'Kost & Vätska', tag: 'shaker', shopify: 'Kost & Vätska', gender: 'unisex' },
  'hydration': { sv: 'Kost & Vätska', tag: 'hydrering', shopify: 'Kost & Vätska', gender: 'unisex' },
  'electrolyte': { sv: 'Kost & Vätska', tag: 'elektrolyter', shopify: 'Kost & Vätska', gender: 'unisex' },
  'meal prep': { sv: 'Kost & Vätska', tag: 'meal prep', shopify: 'Kost & Vätska', gender: 'unisex' },

  // Utrustning & Tillbehör
  'gym bag': { sv: 'Utrustning & Tillbehör', tag: 'väska', shopify: 'Utrustning & Tillbehör', gender: 'unisex' },
  'backpack': { sv: 'Utrustning & Tillbehör', tag: 'ryggsäck', shopify: 'Utrustning & Tillbehör', gender: 'unisex' },
  'gloves': { sv: 'Utrustning & Tillbehör', tag: 'handskar', shopify: 'Utrustning & Tillbehör', gender: 'unisex' },
  'belt': { sv: 'Utrustning & Tillbehör', tag: 'bälte', shopify: 'Utrustning & Tillbehör', gender: 'unisex' },
  'mat': { sv: 'Utrustning & Tillbehör', tag: 'matta', shopify: 'Utrustning & Tillbehör', gender: 'unisex' },
  'rope': { sv: 'Utrustning & Tillbehör', tag: 'rep', shopify: 'Utrustning & Tillbehör', gender: 'unisex' },

};
function mapCategory(rawCategory, productTitle) {
  const text = (rawCategory + ' ' + productTitle).toLowerCase();
  for (const [key, val] of Object.entries(CATEGORY_MAP)) {
    if (text.includes(key)) return val;
  }
  return { sv: 'Träning & Fitness', tag: 'fitness', shopify: 'Träning & Fitness', gender: 'unisex' };
}

// Create Shopify collection if it doesn't exist
const shopifyCollections = {};
async function getOrCreateCollection(name) {
  if (shopifyCollections[name]) return shopifyCollections[name];
  const domain = process.env.SHOPIFY_DOMAIN;
  const token = process.env.SHOP_TOKEN || process.env.SHOPIFY_TOKEN;
  if (!domain || !token) return null;
  try {
    // Check if collection exists
    const res = await axios.get(
      `https://${domain}/admin/api/2024-01/custom_collections.json?title=${encodeURIComponent(name)}`,
      { headers: { 'X-Shopify-Access-Token': token } }
    );
    let col = res.data.custom_collections?.[0];
    if (!col) {
      // Create collection
      const createRes = await axios.post(
        `https://${domain}/admin/api/2024-01/custom_collections.json`,
        { custom_collection: { title: name, published: true } },
        { headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' } }
      );
      col = createRes.data.custom_collection;
      console.log(`✓ Created Shopify collection: ${name}`);
    }
    shopifyCollections[name] = col.id;
    return col.id;
  } catch(e) {
    console.error('Collection error:', e.message);
    return null;
  }
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
    console.log(`✓ Added product ${productId} to collection "${collectionName}"`);
  } catch(e) {
    if (!e.message.includes('taken')) console.error('Collection add error:', e.message);
  }
}


// ── CONTENT FILTER ────────────────────────────────────────
const BLOCKED_KEYWORDS = [
  // Sex / Adult
  'sex','adult','erotic','porn','porno','nude','naked',
  'vibrator','dildo','buttplug','condom',
  'penis','vagina','anal','fetish','bdsm','lingerie sexy',

  // Våld / Vapen
  'gun','weapon','weapons','sword','knife','dagger',
  'bullet','ammo','ammunition','firearm','pistol',
  'rifle','grenade','crossbow','tactical','military weapon',

  // Droger / Alkohol / Tobak
  'drug','drugs','cannabis','marijuana','weed',
  'cocaine','heroin','lsd','mushroom',
  'whiskey','whisky','beer','wine','vodka',
  'cigarette','tobacco','vape','e-cigarette','hookah',

  // Gambling
  'gambling','casino','betting','poker','slot machine',

  // Piratkopior
  'fake','replica','counterfeit','copy version','knockoff',

  // Bil & Motor
  'automobile','automotive','vehicle','motorcycle',
  'speedometer','dashboard','windshield',
  'car mat clip','glovebox','engine part',

  // Hem & Inredning
  'curtain','tablecloth','wall sticker',
  'picture frame','vase','candle holder',
  'home decor','home decoration',

  // Husdjur
  'pet dress','dog dress','cat costume',
  'pet clothes','pet clothing',

  // Beauty / Hudvård
  'beauty','skincare','skin care',
  'facial','face patch','face patches',
  'face mask','anti aging','anti-aging',
  'wrinkle','wrinkles','serum','cleanser',
  'lip balm','lipstick','mascara',
  'foundation','eyeshadow','nail polish',
  'eyelash','cosmetic','cosmetics',
  'makeup','make-up','skin treatment',

  // Smycken / Mode utanför sport
  'jewelry','necklace','earring','bracelet',
  'earring','luxury watch','wedding dress',
  'mattress','latex mattress','bed','bedroom','sofa','pillow','cushion','furniture','garden bench','dress','skirt','jumpsuit','curtain','tapestry','vacuum cleaner','multimeter','car switch','headlight switch',
  'evening dress','tuxedo','high heel','stiletto',

  // Barnprodukter vi inte vill sälja
  'baby bottle','baby diaper','pacifier',

  // Elektronik utanför sport
  'gaming keyboard','gaming mouse',
  'led strip','phone case','tablet case'
];

// Niche validation — product must relate to at least one of these
const NICHE_KEYWORDS = [

  // Gym & Fitness
  'gym',
  'fitness',
  'workout',
  'training',
  'exercise',
  'crossfit',
  'weightlifting',
  'strength training',
  'resistance bands',
  'pull up bar',
  'ab roller',
  'jump rope',
  'gym gloves',
  'weightlifting belt',

  // Running
  'running',
  'runner',
  'running shoes',
  'trail running',
  'road running',
  'running jacket',
  'running shorts',
  'hydration vest',

  // Outdoor
  'outdoor',
  'camping',
  'hiking',
  'trekking',
  'hiking boots',
  'trekking poles',
  'sleeping bag',
  'camping stove',
  'camping tent',
  'outdoor backpack',
  'water filter',

  // Cycling
  'cycling',
  'cyclist',
  'bike helmet',
  'cycling jersey',
  'cycling shorts',
  'cycling gloves',
  'cycling computer',

  // Recovery
  'recovery',
  'foam roller',
  'massage gun',
  'compression',
  'compression sleeve',
  'knee brace',
  'ankle support',
  'back stretcher',
  'muscle recovery',

  // Sport Tech
  'fitness tracker',
  'heart rate monitor',
  'gps watch',
  'sports earbuds',
  'running headphones',
  'smartwatch sport',

  // Apparel
  'training shorts',
  'compression shirt',
  'gym hoodie',
  'running tights',
  'athletic socks',
  'thermal base layer',
  'sportswear',
  'athletic wear',

  // Swedish
  'träning',
  'fitness',
  'gym',
  'löpning',
  'vandring',
  'friluftsliv',
  'cykling',
  'återhämtning',
  'sportkläder',
  'träningskläder'
];

function isProductBlocked(product) {
  const text = [
    product.title, product.nameEn, product.name,
    product.categoryName, product.description
  ].join(' ').toLowerCase();
  
  // Check blocked keywords
  for (const kw of BLOCKED_KEYWORDS) {
    if (text.includes(kw)) {
      console.log(`⛔ Blocked: "${product.title||product.nameEn}" — keyword: "${kw}"`);
      return true;
    }
  }

  // Block car/auto parts
  const catText = (product.categoryName || '').toLowerCase();
  if (catText.includes('automobile') || catText.includes('car care') || 
      catText.includes('vehicle') || catText.includes('motor')) {
    console.log(`⛔ Blocked auto category: "${product.title||product.nameEn}"`);
    return true;
  }

  // Block car brands
  const carBrands = ['ford ','toyota ','honda ','bmw ','mercedes ','audi ','volkswagen '];
  for (const brand of carBrands) {
    if (text.includes(brand)) {
      console.log(`⛔ Blocked car brand: "${product.title||product.nameEn}"`);
      return true;
    }
  }

  // NICHE CHECK — must match at least one fitness/outdoor/health keyword
  const isNicheRelevant = NICHE_KEYWORDS.some(kw => text.includes(kw));
  if (!isNicheRelevant) {
    console.log(`⛔ Off-niche: "${product.title||product.nameEn}" — not fitness/outdoor/health`);
    return true;
  }

  return false;
}

// Auto-publish threshold
const AUTO_PUBLISH_SCORE = 80; // Disabled — all products go to approval queue
const MIN_SCORE = 65;          // Products below 60 are rejected
const MAX_SHIPPING_DAYS = 15;  // Max shipping days — only used when data is available

// ── ALIEXPRESS DATAHUB API (via RapidAPI) ─────────────────
let aliExpressDisabled = false; // Auto-disable if quota exceeded

async function searchAliExpressProducts(keyword, limit = 20) {
  const rapidApiKey = process.env.RAPIDAPI_KEY;
  if (!rapidApiKey || aliExpressDisabled) {
    if (aliExpressDisabled) console.log('AliExpress disabled — monthly quota exceeded');
    return [];
  }
  try {
    const res = await axios.get('https://aliexpress-datahub.p.rapidapi.com/item_search_2', {
      headers: {
        'x-rapidapi-host': 'aliexpress-datahub.p.rapidapi.com',
        'x-rapidapi-key': rapidApiKey
      },
      params: {
        q: keyword,
        page: '1',
        sort: 'SALE_PRICE_ASC'
      }
    });
    return res.data?.result?.resultList || [];
  } catch(e) {
    if (e.response?.status === 429 || e.message?.includes('MONTHLY') || e.message?.includes('quota')) {
      aliExpressDisabled = true;
      console.log('AliExpress monthly quota exceeded — disabling for this session. Upgrade at rapidapi.com');
    } else {
      console.error('AliExpress search failed:', e.message);
    }
    return [];
  }
}

async function getAliExpressProductDetail(itemId) {
  const rapidApiKey = process.env.RAPIDAPI_KEY;
  if (!rapidApiKey) return null;
  try {
    const res = await axios.get('https://aliexpress-datahub.p.rapidapi.com/item_detail_2', {
      headers: {
        'x-rapidapi-host': 'aliexpress-datahub.p.rapidapi.com',
        'x-rapidapi-key': rapidApiKey
      },
      params: { itemId: itemId.toString() }
    });
    return res.data?.result || null;
  } catch(e) {
    console.error('AliExpress detail failed:', e.message);
    return null;
  }
}

// ── CJ DROPSHIPPING API ───────────────────────────────────
async function getCJToken() {
  // Try API key first (new method)
  if (process.env.CJ_API_KEY) {
    try {
      const res = await axios.post('https://developers.cjdropshipping.com/api2.0/v1/authentication/getAccessToken', {
        apiKey: process.env.CJ_API_KEY
      });
      if (res.data?.data?.accessToken) {
        console.log('✓ CJ token acquired via API key');
        return res.data.data.accessToken;
      }
      console.error('CJ API key auth failed:', res.data?.message);
    } catch(e) {
      console.error('CJ API key auth error:', e.message);
    }
  }
  // Fallback to email/password
  if (process.env.CJ_EMAIL && process.env.CJ_PASSWORD) {
    try {
      const res = await axios.post('https://developers.cjdropshipping.com/api2.0/v1/authentication/getAccessToken', {
        email: process.env.CJ_EMAIL,
        password: process.env.CJ_PASSWORD
      });
      if (res.data?.data?.accessToken) {
        console.log('✓ CJ token acquired via email/password');
        return res.data.data.accessToken;
      }
      console.error('CJ email auth failed:', res.data?.message);
    } catch(e) {
      console.error('CJ email auth error:', e.message);
    }
  }
  return null;
}

async function searchCJProducts(token, keyword, limit = 20) {
  try {
    const res = await axios.get('https://developers.cjdropshipping.com/api2.0/v1/product/list', {
      headers: { 'CJ-Access-Token': token },
      params: { productNameEn: keyword, pageNum: 1, pageSize: limit, orderBy: 'ORDER_COUNT', orderType: 'DESC' }
    });
    const list = res.data?.data?.list || [];
    console.log(`CJ "${keyword}": ${list.length} results (msg: ${res.data?.message})`);
    return list;
  } catch(e) {
    if (e.response?.status === 429) {
      console.error('CJ rate limit hit — waiting...');
    } else {
      console.error('CJ search failed:', e.response?.status, e.message);
    }
    return [];
  }
}

function scoreCJProduct(product, index) {
  let score = 40;

  const text = [
    product.nameEn,
    product.name,
    product.categoryName,
    product.description
  ].join(' ').toLowerCase();

  // Pris
  const price = parseFloat(product.sellPrice) || 10;
  if (price >= 5 && price <= 80) score += 20;

  // Ordrar
  const orders = parseInt(product.orderCount) || 0;
  score += Math.min(20, orders / 50);

  // Bilder
  const imgs = (product.productImageSet || []).length;
  score += Math.min(10, imgs * 2);

  // Prioriterade Meloni-kategorier
  const premiumKeywords = [
    'running shoes',
    'trail running',
    'gym shoes',
    'training shoes',
    'hiking boots',
    'outdoor backpack',
    'gym bag',
    'compression',
    'massage gun',
    'foam roller',
    'cycling jersey',
    'cycling shorts',
    'fitness tracker',
    'heart rate monitor',
    'fitness tracker',
    'gps watch',
    'heart rate monitor',
    'smartwatch sport',
    'sports earbuds',
    'running headphones',
    'cycling computer',
    'action camera',
    'solar charger outdoor',
    'bone conduction headphones'
  ];

  const goodKeywords = [
    'running',
    'training',
    'fitness',
    'gym',
    'hiking',
    'trekking',
    'camping',
    'cycling',
    'recovery',
    'sportswear'
  ];

  const weakKeywords = [
    'casual',
    'fashion',
    'lifestyle',
    'daily wear',
    'streetwear',
    'sandals',
    'sneakers'
  ];

  premiumKeywords.forEach(k => {
    if (text.includes(k)) score += 15;
  });

  goodKeywords.forEach(k => {
    if (text.includes(k)) score += 5;
  });

  weakKeywords.forEach(k => {
    if (text.includes(k)) score -= 10;
  });

  // Rank bonus
  score += Math.max(0, 10 - index);

  return Math.max(0, Math.min(100, Math.round(score)));
}

// ── SHOPIFY API ────────────────────────────────────────────
async function publishToShopify(product) {
  const domain = process.env.SHOPIFY_DOMAIN;
  const token  = process.env.SHOP_TOKEN || process.env.SHOPIFY_TOKEN;
  if(!domain || !token) throw new Error('Shopify not configured');

  const payload = {
    product: {
      title: product.title,
      body_html: product.descriptionHtml,
      vendor: 'Meloni',
      product_type: product.category,
      tags: product.tags?.join(','),
      status: 'active',
      variants: [{
        price: product.sellPrice.toString(),
        sku: `VIN-${product.aliId || product.cjPid || 'AUTO'}`,
        inventory_management: 'shopify',
        inventory_quantity: product.stock || 50,
        weight: product.weight || 0.3,
        weight_unit: 'kg'
      }],
      metafields: [
        {
          namespace: 'custom',
          key: 'shipping_days',
          value: String(product.shippingDays || 10),
          type: 'number_integer'
        },
        {
          namespace: 'custom',
          key: 'source',
          value: product.source || 'cj',
          type: 'single_line_text_field'
        }
      ],
      images: (product.images||[]).filter(u=>u&&u.startsWith('http')).slice(0, 5).map(url => ({ src: url, position: (product.images||[]).indexOf(url) + 1 }))
    }
  };

  const res = await axios.post(
    `https://${domain}/admin/api/2024-01/products.json`,
    payload,
    { headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' } }
  );
  return res.data.product;
}

// ── OPENAI CONTENT GENERATION ──────────────────────────────
async function generateProductContent(rawProduct) {
  const prompt = `Du är en erfaren svensk copywriter för Vintera — en modern, kurerad livsstilsbutik. Du skriver som en riktig svensk människa, inte som en översättning från engelska. Naturlig, varm och trovärdig svenska.

Produkt: ${rawProduct.nameEn || rawProduct.name}
Kategori: ${rawProduct.categoryName || 'Allmänt'}
Beskrivning: ${rawProduct.description || 'Inte angiven'}

Skriv EXAKT i detta format (all text på svenska):

TITLE: [Kort, naturlig svensk titel — max 7 ord. Skriv som en svensk skulle säga det, t.ex. "Trådlös laddare för bilen" inte "Wireless Car Charger Pro Max"]

META: [SEO-beskrivning, max 155 tecken. Naturlig svenska, fördel-först. Inga utropstecken.]

DESCRIPTION: [2 korta stycken. Skriv avslappnat och trovärdigt — som om du tipsar en vän. Ingen reklamsvenska, inga överdrifter som "revolutionerande" eller "banbrytande". Fokusera på hur produkten faktiskt hjälper i vardagen.]

BENEFITS:
• [Konkret fördel — kort och tydlig, max 12 ord]
• [Konkret fördel — kort och tydlig, max 12 ord]
• [Konkret fördel — kort och tydlig, max 12 ord]

FAQ:
Q: [Den vanligaste frågan en svensk kund skulle ställa]
A: [Kort, ärligt svar. Inga löften du inte kan hålla.]

AD_HOOK: [En rad för Instagram/TikTok — max 10 ord. Ska kännas äkta, inte som reklam. T.ex. "Därför har alla börjat använda den här" eller "Äntligen slipper du det här problemet"]

TAGS: [5 taggar, gemener, relevanta, på svenska]

Viktigt: Inga engelska ord om det finns ett bra svenskt alternativ. Inget "dropshipping", inga leverantörsnamn, inga påhittade specifikationer.`;

  const res = await openai.chat.completions.create({
    model: 'gpt-4o',
    max_tokens: 900,
    messages: [
      { role: 'system', content: 'Du är en erfaren svensk copywriter specialiserad på aktiv livsstil, träning, friluftsliv och hälsa. Du skriver naturlig, modern svenska för en sportig målgrupp — inte översatt engelska. Returnera endast det formaterade innehållet, inget annat.' },
      { role: 'user', content: prompt }
    ]
  });

  return res.choices[0].message.content;
}

function parseGeneratedContent(raw) {
  const get = (key, next) => {
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

  const descriptionHtml = `<p>${description.replace(/\n\n/g, '</p><p>')}</p>
<ul>${benefits.split('\n').filter(l => l.trim().startsWith('•')).map(l => `<li>${l.replace('•','').trim()}</li>`).join('')}</ul>
<h4>FAQ</h4>${faq}`;

  return {
    title,
    meta,
    description,
    descriptionHtml,
    benefits: benefits.split('\n').filter(l => l.trim().startsWith('•')).map(l => l.replace('•','').trim()),
    adHook,
    tags: tagsRaw.split(',').map(t => t.trim()).filter(Boolean)
  };
}

// ── SCORING ENGINE ─────────────────────────────────────────
function scoreAliProduct(product, index) {
  let score = 0;
  // API returns {item: {...}} wrapper — unwrap if needed
  const p = product.item || product;
  const price = parseFloat(p.sku?.def?.promotionPrice || p.sku?.def?.price || 5);
  // Price score (max 20pts) — ignore suspiciously low prices
  if (price >= 2 && price <= 80) score += 20;
  else if (price > 0) score += 8;
  // Sales (max 30pts)
  const sales = parseInt(p.sales || p.trade?.realTrade || 0);
  score += Math.min(30, sales / 50);
  // Rating (max 20pts)
  const rating = parseFloat(p.averageStarRate || p.averageStar || 0);
  score += Math.round(rating * 4);
  // Image (max 15pts)
  if (p.image || p.imageUrl) score += 15;
  // Position bonus (max 15pts)
  score += Math.max(0, 15 - index);
  return Math.min(100, Math.round(score));
}

function scoreProduct(product, index) {
  let score = 0;

  // Margin score (max 25pts)
  const costUsd   = parseFloat(product.sellPrice) || 10;
  const sellPrice = costUsd * 5; // 5x default
  const margin    = ((sellPrice - costUsd) / sellPrice) * 100;
  score += Math.min(25, margin / 3);

  // Order count (max 20pts) — popularity signal
  const orders = parseInt(product.orderCount) || 0;
  score += Math.min(20, orders / 100);

  // Images available (max 15pts)
  const imgCount = product.productImageSet?.length || 0;
  score += Math.min(15, imgCount * 3);

  // Shipping time (max 15pts) — shorter = better
  const shipDays = parseInt(product.shippingTime) || 20;
  score += Math.max(0, 15 - shipDays);

  // Has variants (max 10pts)
  if (product.variants?.length > 1) score += 10;

  // Position bonus — higher ranked = trending (max 15pts)
  score += Math.max(0, 15 - index);

  return Math.min(100, Math.round(score));
}

// ── TREND KEYWORDS ─────────────────────────────────────────
const TREND_KEYWORDS = [

  // ── HERR ─────────────────────────────
  'men running jacket',
  'men gym hoodie',
  'men training shorts',
  'men compression shirt',
  'men hiking pants',
  'men running shoes',
  'men trail running shoes',
  'men gym shoes',
  'men thermal base layer',
  'men athletic socks',

  // ── DAM ──────────────────────────────
  'women running jacket',
  'women gym hoodie',
  'women training shorts',
  'women running tights',
  'women compression leggings',
  'women running shoes',
  'women trail running shoes',
  'women gym shoes',
  'women thermal base layer',
  'women athletic socks',

  // ── GYM & FITNESS ────────────────────
  'resistance bands',
  'pull up bar',
  'ab roller',
  'jump rope',
  'gym gloves',
  'weightlifting belt',
  'fitness tracker',
  'heart rate monitor',
  'gym bag',
  'protein shaker',

  // ── LÖPNING ──────────────────────────
  'running hydration vest',
  'running belt',
  'running cap',
  'road running shoes',
  'trail running shoes',
  'running backpack',

  // ── OUTDOOR ──────────────────────────
  'hiking boots',
  'trekking poles',
  'camping tent',
  'sleeping bag',
  'outdoor backpack',
  'waterproof jacket',
  'camping stove',
  'water filter outdoor',
  'camping lantern',
  'camping cookware',

  // ── CYKLING ──────────────────────────
  'bike helmet',
  'cycling gloves',
  'cycling jersey',
  'cycling shorts',
  'cycling backpack',
  'cycling computer',

  // ── RECOVERY ─────────────────────────
  'massage gun',
  'foam roller',
  'knee brace support',
  'back stretcher',
  'compression sleeve',
  'ankle support',
  'muscle roller stick',

// ── SMART SPORT TECH ─────────────────
'gps watch',
'running headphones',
'sports earbuds wireless',
'action camera',
'solar charger outdoor',
'bone conduction headphones',
'cycling computer',
'bike computer',
'sport watch',
'running watch',
'fitness watch',
'outdoor gps',
'solar charger',
'headlamp',
'smart ring',
'smartwatch',
'smartwatch gps',
'fitness smartwatch',
'gps running watch',
'triathlon watch',
'trail running watch',
'cycling gps',
'gps cycling computer',
'heart rate strap',
'chest heart rate monitor',
'fitness band',
'sleep tracker',
'recovery tracker',
'smart scale',
'body composition scale',
'wireless sports headphones',
'bluetooth sports earbuds',
'waterproof earbuds',
'open ear headphones',
'action cam',
'action cam 4k',
'helmet camera',
'sports camera',
'portable power station',
'camping power bank',
'solar power bank',
'portable solar charger',
'led headlamp',
'usb headlamp',
'camping flashlight',
'bike gps',
'bike radar',
'bike camera',
'cycling sensor',
'cadence sensor',
'speed sensor bike',
'weighted vest',
'wrist wraps',
'lifting straps',
'squat pad',
'workout towel',
'ice bath',
'ice bath tub',
'recovery sandals',
'posture corrector',
'neck massager',
'running sunglasses',
'reflective vest',
'running socks',
'carbon running shoes',
'camping chair',
'camping table',
'emergency radio',
'survival flashlight',
'bike phone mount',
'bike repair kit',
'bike pump',
'bike bottle cage',
];

// ── MAIN RESEARCH PIPELINE ─────────────────────────────────
async function runProductResearch() {
  if (store.syncStatus === 'running') return;
  store.syncStatus = 'running';
  store.lastSync = new Date().toISOString();
  console.log(`[${new Date().toISOString()}] Starting product research...`);

  try {
    const shuffle = arr => [...arr].sort(() => Math.random() - 0.5);
  const delay = ms => new Promise(r => setTimeout(r, ms));
  const keywords = shuffle(TREND_KEYWORDS).slice(0, 150);
    console.log('Researching keywords:', keywords);

    let candidates = [];

    // ── SOURCE 1: AliExpress ──
    if (process.env.RAPIDAPI_KEY) {
      console.log('Searching AliExpress...');
      for (const keyword of keywords) {
        const products = await searchAliExpressProducts(keyword, 10);
        store.stats.totalScanned += products.length;
        products.forEach((p, i) => {
          const score = scoreAliProduct(p, i);
          if (score >= 40) {
            const item = p.item || p;
            candidates.push({ ...item, score, keyword, source: 'aliexpress', _raw: p });
          }
        });
      }
      console.log(`AliExpress: ${candidates.length} candidates found`);
    } else {
      console.log('No RAPIDAPI_KEY — skipping AliExpress');
    }

    // ── SOURCE 2: CJ Dropshipping ──
    if (process.env.CJ_EMAIL && process.env.CJ_PASSWORD) {
      console.log('Searching CJ Dropshipping...');
      const cjToken = await getCJToken();
      if (cjToken) {
      const cjKeywords = shuffle(TREND_KEYWORDS).slice(0, 150);
        for (const keyword of cjKeywords) {
          await delay(2000); // 2s between CJ requests to avoid rate limits
          const products = await searchCJProducts(cjToken, keyword, 10);
          store.stats.totalScanned += products.length;
          products.forEach((p, i) => {
            const score = scoreCJProduct(p, i);
            if (score >= 20) {
              candidates.push({
                ...p,
                score,
                keyword,
                source: 'cj',
                title: p.nameEn || p.name || p.productNameEn || '',
                itemId: p.pid,
                image: p.productImage || (p.productImageSet || [])[0] || '',
                images: p.productImage ? [p.productImage] : (p.productImageSet || []),
                salePrice: p.sellPrice,
                costPrice: parseFloat(p.sellPrice) || 5,
              });
            }
          });
        }
        console.log(`CJ: total ${candidates.length} combined candidates`);
      }
    } else {
      console.log('No CJ credentials — skipping CJ');
    }

    if (candidates.length === 0) {
      console.log('No candidates found from any source');
      store.syncStatus = 'idle';
      return;
    }

    // Sort by score, take top 5
    candidates.sort((a, b) => b.score - a.score);
    const top = candidates.slice(0, 300);

    console.log(`Found ${candidates.length} candidates, processing top ${top.length}`);

    for (const product of top) {
      // Skip if already in queue or approved
      // Block inappropriate products
      if (isProductBlocked(product)) {
        store.stats.totalRejected++;
        continue;
      }

      // Skip if exact ID match
      const idExists = [...store.queue, ...store.products].find(
        p => p.aliId === String(product.itemId||product.productId||product.pid)
      );
     if (idExists) {
  console.log(`⛔ Duplicate ID: ${product.title || product.nameEn}`);
  continue;
}

      // Skip if very similar title already in queue (duplicate from different source)
      const productTitle = (product.title||product.nameEn||'').toLowerCase().slice(0,30);
      const titleExists = productTitle.length > 5 && [...store.queue, ...store.products].find(
        p => p.rawTitle?.toLowerCase().slice(0,30) === productTitle
      );
     if (titleExists) {
  console.log(`⛔ Duplicate Title: ${product.title || product.nameEn}`);
  continue;
}

      try {
        // Get full product details + images
        // Get images — handle both AliExpress and CJ formats
        const fixUrl = u => u ? (u.startsWith('//') ? 'https:' + u : u) : null;
        const images = [];
        const isCJProduct = product.source === 'cj';

        if (isCJProduct) {
          // Skip index 0 (often marketing image with logos/text) - start from index 1
          // But save it as fallback
          const primaryFallback = product.productImage || product.image || '';
          // Try to get more images from CJ product detail API
          try {
            const cjToken = await getCJToken();
            if (cjToken && (product.pid || product.aliId)) {
              const detailRes = await axios.get('https://developers.cjdropshipping.com/api2.0/v1/product/query', {
                headers: { 'CJ-Access-Token': cjToken },
                params: { pid: product.pid || product.aliId }
              });
              const detail = detailRes.data?.data;
              if (detail) {
                // Get all product images
                const detailImgs = detail.productImageSet || [];
                detailImgs.forEach(img => {
                  if (img && typeof img === 'string' && !images.includes(img)) images.push(img);
                });
                // Get variant images too
                const variants = detail.variants || detail.productVariants || [];
                variants.forEach(v => {
                  if (v.variantImage && !images.includes(v.variantImage)) images.push(v.variantImage);
                });
              }
            }
          } catch(e) {}
          // Use pre-stored images, skip first (marketing image)
          const cjImgs = product.images || product.productImageSet || [];
          // Start from index 1 to skip typical marketing/hero image
          const cleanImgs = cjImgs.slice(1);
          cleanImgs.forEach(img => { if(img && typeof img === 'string' && !images.includes(img)) images.push(img); });
          // If we got no images, use primary as fallback
          if (images.length === 0 && primaryFallback) images.push(primaryFallback);
          // Add first image at end as last resort
          else if (images.length < 3 && cjImgs[0] && !images.includes(cjImgs[0])) images.push(cjImgs[0]);
        } else {
          // AliExpress images
          const imgBase = fixUrl(product.image || product.imageUrl || '');
          if (imgBase) images.push(imgBase);
          try {
            const detail = await getAliExpressProductDetail(product.itemId||product.productId);
            if (detail?.imageUrl) {
              const dImg = fixUrl(detail.imageUrl);
              if (dImg && !images.includes(dImg)) images.push(dImg);
            }
          } catch(e) {}
        }

        const costUsd = Math.max(2, parseFloat(product.sku?.def?.promotionPrice || product.sku?.def?.price || product.price?.minPrice || product.salePrice || 5));
        const rawSek = Math.round(costUsd * 5 * 9.5); // 5x markup, USD to SEK
        const sellSek = snapPrice(rawSek);

        // Generate AI content
        const productName = product.title || product.nameEn || product.name || product.subject || product.productNameEn || 'Trending Product';
        const productDesc = product.productNameEn || product.nameEn || product.name || productName;
        const rawContent = await generateProductContent({
          nameEn: productName,
          description: productDesc,
          sellPrice: costUsd,
          categoryName: product.productType || product.categoryName || product.category || 'General'
        });
        const content = parseGeneratedContent(rawContent);

        // Reject low-score products
if (product.score < MIN_SCORE) {
  console.log(`⛔ Low score: "${product.title || product.nameEn}" (${product.score})`);
  store.stats.totalRejected++;
  continue;
}

        const queueItem = {
          id: `q_${Date.now()}_${Math.random().toString(36).slice(2,7)}`,
          aliId: String(product.itemId||product.productId||Math.random()),
          score: product.score,
          keyword: product.keyword,
          costPrice: costUsd,
          sellPrice: sellSek,
          margin: Math.round(((sellSek - costUsd*9.5) / sellSek) * 100),
          category: product.productType || 'General',
          stock: 99,
          shippingDays: 14,
          images: images.slice(0,5),
          // AI-generated content
          title: content.title || product.title,
          meta: content.meta,
          description: content.description,
          descriptionHtml: content.descriptionHtml,
          benefits: content.benefits,
          adHook: content.adHook,
          tags: [...(content.tags||[]), mapCategory(product.categoryName || product.productType || '', product.title || product.nameEn || '').tag],
          // Raw
          rawTitle: product.title || product.name || product.subject || 'Product',
          aliUrl: `https://www.aliexpress.com/item/${product.itemId||product.productId}.html`,
          addedAt: new Date().toISOString(),
          status: 'pending',
          autoPublish: product.score >= AUTO_PUBLISH_SCORE
        };

        // Auto-publish high-score products directly to Shopify
        if (queueItem.autoPublish) {
          console.log(`🚀 Auto-publishing: ${content.title} (score: ${product.score})`);
          try {
            const shopifyProduct = await publishToShopify(queueItem);
            queueItem.status = 'approved';
            queueItem.shopifyId = shopifyProduct.id;
            // Register in scoring system
            if (registerNewProduct && db) {
              registerNewProduct(shopifyProduct.id, shopifyProduct.title, db).catch(()=>{});
            }
            queueItem.approvedAt = new Date().toISOString();
            store.products.push(queueItem);
            store.stats.totalApproved++;
            await dbSave('products', queueItem);
            // Add to collections
            try {
              const { assignCollections } = require('./collectionAssign');
              assignCollections(shopifyProduct.id, queueItem.title, queueItem.tags, db).catch(()=>{});
            } catch(e) {}
            console.log(`✓ Auto-published: ${content.title} → Shopify ID ${shopifyProduct.id}`);
            // Trigger integrations
            triggerMakeWebhook('product_approved', { title: queueItem.title, price: queueItem.sellPrice, shopifyId: shopifyProduct.id, images: queueItem.images?.slice(0,1) });
            sendEmailNotification(`🚀 Auto-published: ${queueItem.title}`, `<h2>${queueItem.title}</h2><p>Score: ${queueItem.score}/100 — Auto-published to Vintera</p>${queueItem.images?.[0]?`<img src="${queueItem.images[0]}" style="max-width:300px;border-radius:8px"/>`:''}`);
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

        // Notify via Make + Email when product added to queue
        triggerMakeWebhook('product_queued', { title: queueItem.title, score: queueItem.score, category: queueItem.category, image: queueItem.images?.[0] });
        sendEmailNotification(
          `🔍 New product ready for review: ${queueItem.title}`,
          `<h2>New product in approval queue</h2>
           <p><b>${queueItem.title}</b></p>
           <p><b>Score:</b> ${queueItem.score}/100 &nbsp;|&nbsp; <b>Margin:</b> ${queueItem.margin}% &nbsp;|&nbsp; <b>Category:</b> ${queueItem.category}</p>
           ${queueItem.images?.[0] ? `<img src="${queueItem.images[0]}" style="max-width:280px;border-radius:8px;margin:12px 0"/>` : ''}
           <p style="color:#86868b;font-size:13px">Log in to Mercury to review and approve.</p>`
        );

      } catch(contentErr) {
        console.error('Content generation failed for', product.pid, contentErr.message);
      }
    }

    store.syncStatus = 'idle';
    console.log(`Research complete. Queue: ${store.queue.length} items`);

  } catch(err) {
    console.error('Research pipeline error:', err.message);
    store.syncStatus = 'error';
  }
}


// ── ORBIT INTEGRATION ─────────────────────────────────────
async function triggerOrbitCampaign(product) {
  const orbitUrl   = process.env.ORBIT_API_URL;
  const orbitToken = process.env.ORBIT_API_TOKEN;
  const orbitBrand = process.env.ORBIT_BRAND_ID;

  if (!orbitUrl || !orbitToken || !orbitBrand) {
    console.log('Orbit not configured — skipping campaign trigger');
    return null;
  }

  try {
    // Create campaign draft in Orbit
    const campaignRes = await axios.post(
      `${orbitUrl}/api/v1/campaigns`,
      {
        brand_id: orbitBrand,
        name: `${product.title} — Launch`,
        type: 'product_launch',
        status: 'draft',
        product: {
          title: product.title,
          description: product.description,
          price: `${product.sellPrice} kr`,
          category: product.category,
          tags: product.tags,
          images: product.images?.slice(0,5),
          ad_hook: product.adHook,
          benefits: product.benefits,
          shopify_id: product.shopifyId,
          meta_description: product.meta,
        },
        goals: {
          objective: 'product_awareness',
          channels: ['instagram','tiktok','meta_ads'],
        }
      },
      { headers: { 'Authorization': `Bearer ${orbitToken}`, 'Content-Type': 'application/json' } }
    );

    const campaign = campaignRes.data;
    console.log(`✓ Orbit campaign created: ${campaign.id || campaign.job_id}`);

    // Trigger content generation
    if (campaign.id) {
      await axios.post(
        `${orbitUrl}/api/v1/content/generate`,
        {
          brand_id: orbitBrand,
          campaign_id: campaign.id,
          formats: ['instagram_post','tiktok_caption','meta_ad_copy','story'],
          product_context: {
            title: product.title,
            ad_hook: product.adHook,
            benefits: product.benefits,
            images: product.images?.slice(0,3),
          }
        },
        { headers: { 'Authorization': `Bearer ${orbitToken}`, 'Content-Type': 'application/json' } }
      );
      console.log(`✓ Orbit content generation triggered`);
    }

    return campaign;
  } catch(err) {
    console.error('Orbit trigger failed (non-blocking):', err.message);
    return null;
  }
}


// ── MAKE.COM WEBHOOK ──────────────────────────────────────
async function triggerMakeWebhook(event, data) {
  const webhookUrl = process.env.MAKE_WEBHOOK_URL;
  if (!webhookUrl) return null;

  try {
    const payload = {
      event,           // product_approved | product_queued | low_stock | research_complete
      timestamp: new Date().toISOString(),
      store: process.env.SHOPIFY_DOMAIN,
      data
    };
    await axios.post(webhookUrl, payload, {
      headers: { 'Content-Type': 'application/json' }
    });
    console.log(`✓ Make.com webhook sent: ${event}`);
  } catch(err) {
    console.error('Make webhook failed (non-blocking):', err.message);
  }
}

// ── EMAIL NOTIFICATIONS (via Resend) ─────────────────────
async function sendEmailNotification(subject, html) {
  const resendKey = process.env.RESEND_API_KEY;
  const notifyEmail = process.env.NOTIFY_EMAIL;
  if (!resendKey || !notifyEmail) return null;

  try {
    await axios.post('https://api.resend.com/emails', {
      from: 'Mercury AI <mercury@vintera.store>',
      to: notifyEmail,
      subject,
      html
    }, {
      headers: {
        'Authorization': `Bearer ${resendKey}`,
        'Content-Type': 'application/json'
      }
    });
    console.log(`✓ Email sent: ${subject}`);
  } catch(err) {
    console.error('Email failed (non-blocking):', err.message);
  }
}

// ── SHOPIFY PERFORMANCE TRACKING ─────────────────────────
async function syncShopifyPerformance() {
  const domain = process.env.SHOPIFY_DOMAIN;
  const token  = process.env.SHOP_TOKEN || process.env.SHOPIFY_TOKEN;
  if (!domain || !token) return;

  try {
    // Fetch last 30 days orders
    const since = new Date(Date.now() - 30*24*60*60*1000).toISOString();
    const ordersRes = await axios.get(
      `https://${domain}/admin/api/2024-01/orders.json?status=any&created_at_min=${since}&limit=250`,
      { headers: { 'X-Shopify-Access-Token': token } }
    );

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

    // Update store products with real sales data
    store.products.forEach(p => {
      const shopifyId = p.shopifyId?.toString();
      if (shopifyId && productSales[shopifyId]) {
        p.unitsSold   = productSales[shopifyId].units;
        p.revenue30d  = Math.round(productSales[shopifyId].revenue);
      }
    });

    store.performance = {
      revenue30d: Math.round(totalRevenue),
      orderCount: orders.filter(o => o.financial_status === 'paid').length,
      lastUpdated: new Date().toISOString()
    };

    console.log(`✓ Performance synced: ${store.performance.orderCount} orders, ${store.performance.revenue30d} kr revenue`);

  } catch(err) {
    console.error('Performance sync failed:', err.message);
  }
}

// ── API ROUTES ─────────────────────────────────────────────

// ── SHOPIFY OAUTH ─────────────────────────────────────────
app.get('/api/auth', (req, res) => {
  const apiKey = process.env.SHOPIFY_API_KEY;
  const scopes = 'read_products,write_products,read_orders,write_orders,read_inventory,write_inventory';
  const redirectUri = `https://mercury-production-ace6.up.railway.app/api/auth/callback`;
  const shop = req.query.shop || process.env.SHOPIFY_DOMAIN;
  const authUrl = `https://${shop}/admin/oauth/authorize?client_id=${apiKey}&scope=${scopes}&redirect_uri=${encodeURIComponent(redirectUri)}`;
  res.redirect(authUrl);
});

app.get('/api/auth/callback', async (req, res) => {
  const { code, shop } = req.query;
  const apiKey = process.env.SHOPIFY_API_KEY;
  const apiSecret = process.env.SHOPIFY_API_SECRET;
  try {
    const tokenRes = await axios.post(`https://${shop}/admin/oauth/access_token`, {
      client_id: apiKey,
      client_secret: apiSecret,
      code
    });
    const accessToken = tokenRes.data.access_token;
    console.log(`✓ OAuth complete for ${shop}. Token: ${accessToken.slice(0,10)}...`);
    console.log('Add to Railway variables: SHOPIFY_TOKEN=' + accessToken);
    // Store temporarily
    process.env.SHOP_TOKEN = accessToken;
    process.env.SHOPIFY_TOKEN = accessToken;
    res.send(`<h2>✓ Connected!</h2><p>Token acquired for ${shop}</p><p>Add this to Railway Variables as SHOP_TOKEN:</p><code style="background:#f0f0f0;padding:10px;display:block;margin:10px 0;word-break:break-all">${accessToken}</code><p>Then redeploy Railway.</p>`);
  } catch(e) {
    res.status(500).send('OAuth failed: ' + e.message);
  }
});

// Status
app.get('/api/status', (req, res) => {
  res.json({
    ok: true,
    syncStatus: store.syncStatus,
    lastSync: store.lastSync,
    queueCount: store.queue.length,
    productCount: store.products.length,
    stats: store.stats
  });
});

// Get approval queue
app.get('/api/queue', (req, res) => {
  res.json(store.queue.filter(p => p.status === 'pending'));
});

// Get live products
app.get('/api/products', (req, res) => {
  res.json(store.products);
});

// Approve product → publish to Shopify
app.post('/api/approve/:id', async (req, res) => {
  const item = store.queue.find(p => p.id === req.params.id);
  if (!item) return res.status(404).json({ error: 'Not found' });

  try {
    // Apply any manual edits from request body
    if (req.body.title)       item.title = req.body.title;
    if (req.body.description) item.description = req.body.description;
    if (req.body.sellPrice)   item.sellPrice = req.body.sellPrice;

    const shopifyProduct = await publishToShopify(item);
    item.status = 'approved';
    item.shopifyId = shopifyProduct.id;
    // Register in scoring system
    if (registerNewProduct && db) {
      registerNewProduct(shopifyProduct.id, shopifyProduct.title, db).catch(e => console.error('Score register failed:', e.message));
    }
    item.approvedAt = new Date().toISOString();

    store.products.push(item);
    store.queue = store.queue.filter(p => p.id !== req.params.id);
    store.stats.totalApproved++;
    await dbSave('products', item);
    await dbDelete('queue', item.id);

    // Add to Shopify collections via collectionAssign
    try {
      const { assignCollections } = require('./collectionAssign');
      assignCollections(shopifyProduct.id, item.title, item.tags, db).catch(e => console.error('Collection assign error:', e.message));
    } catch(e) { console.log('collectionAssign not loaded'); }

    // Trigger all integrations in background (non-blocking)
    Promise.all([
      // Orbit campaign
      triggerOrbitCampaign(item).then(campaign => {
        if (campaign) item.orbitCampaignId = campaign.id || campaign.job_id;
      }),
      // Make.com webhook — triggers social posts, ads, etc.
      triggerMakeWebhook('product_approved', {
        title: item.title,
        description: item.description,
        adHook: item.adHook,
        price: item.sellPrice,
        category: item.category,
        tags: item.tags,
        images: item.images?.slice(0,3),
        shopifyId: item.shopifyId,
        shopifyUrl: `https://${process.env.SHOPIFY_DOMAIN}/products/${item.shopifyId}`,
      }),
      // Email notification
      sendEmailNotification(
        `✓ New product live: ${item.title}`,
        `<h2>${item.title}</h2>
         <p><b>Score:</b> ${item.score}/100 &nbsp;|&nbsp; <b>Price:</b> ${item.sellPrice} kr &nbsp;|&nbsp; <b>Category:</b> ${item.category}</p>
         <p><b>Ad hook:</b> ${item.adHook || '—'}</p>
         ${item.images?.[0] ? `<img src="${item.images[0]}" style="max-width:300px;border-radius:8px;margin:12px 0"/>` : ''}
         <p><a href="https://${process.env.SHOPIFY_DOMAIN}/products/${item.shopifyId}" style="background:#0071e3;color:white;padding:10px 20px;border-radius:6px;text-decoration:none">View in Shopify →</a></p>`
      )
    ]).catch(err => console.error('Integration error:', err.message));

    res.json({ ok: true, shopifyId: shopifyProduct.id, product: item });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// Reject product
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

// Regenerate copy for a queued product
app.post('/api/regenerate/:id', async (req, res) => {
  const item = store.queue.find(p => p.id === req.params.id);
  if (!item) return res.status(404).json({ error: 'Not found' });
  try {
    const rawContent = await generateProductContent({ nameEn: item.rawTitle, description: item.description, sellPrice: item.costPrice });
    const content = parseGeneratedContent(rawContent);
    item.title       = content.title;
    item.description = content.description;
    item.descriptionHtml = content.descriptionHtml;
    item.benefits    = content.benefits;
    item.adHook      = content.adHook;
    item.tags        = content.tags;
    res.json({ ok: true, item });
  } catch(err) {
    res.status(500).json({ error: err.message });
  }
});

// Trigger manual research run
app.post('/api/research/run', async (req, res) => {
  res.json({ ok: true, message: 'Research started' });
  runProductResearch(); // run in background
});

// Get performance data
app.get('/api/performance', (req, res) => {
  res.json({
    performance: store.performance || null,
    products: store.products.map(p => ({
      id: p.id, title: p.title, shopifyId: p.shopifyId,
      unitsSold: p.unitsSold || 0, revenue30d: p.revenue30d || 0,
      sellPrice: p.sellPrice, score: p.score
    }))
  });
});

// Manual performance sync
app.post('/api/performance/sync', async (req, res) => {
  res.json({ ok: true, message: 'Syncing performance data...' });
  syncShopifyPerformance();
});

// Dashboard stats
app.get('/api/dashboard', (req, res) => {
  const revenue = store.products.reduce((s, p) => s + (p.sellPrice || 0), 0);
  res.json({
    syncStatus: store.syncStatus,
    lastSync: store.lastSync,
    queueCount: store.queue.length,
    productCount: store.products.length,
    stats: store.stats,
    performance: store.performance || null,
    recentQueue: store.queue.slice(-3),
    topProducts: [...store.products].sort((a,b)=>(b.revenue30d||0)-(a.revenue30d||0)).slice(0,5)
  });
});


// ── SKAPA ALLA KOLLEKTIONER (engångskörning) ──────────────
app.get('/api/create-collections', async (req, res) => {
  const domain = process.env.SHOPIFY_DOMAIN;
  const token  = process.env.SHOP_TOKEN || process.env.SHOPIFY_TOKEN;
  if (!domain || !token) return res.status(500).json({ error: 'Shopify not configured' });

  const COLLECTIONS = [
  // ── TOPPNIVÅ ─────────────────────────────
  { title: 'Nyheter', handle: 'nyheter', sort: 'created-descending' },
  { title: 'Bästsäljare', handle: 'bestsellers', sort: 'best-selling' },
  { title: 'Rea', handle: 'rea', sort: 'price-descending' },

  // ── KÖN ─────────────────────────────────
  { title: 'Herr', handle: 'herr' },
  { title: 'Dam', handle: 'dam' },
  { title: 'Barn', handle: 'barn' },

  // ── HUVUDKATEGORIER ─────────────────────
  { title: 'Träning & Fitness', handle: 'traning-fitness' },
  { title: 'Löpning', handle: 'lopning' },
  { title: 'Outdoor & Camping', handle: 'outdoor-camping' },
  { title: 'Vandring', handle: 'vandring' },
  { title: 'Cykling', handle: 'cykling' },
  { title: 'Yoga & Pilates', handle: 'yoga-pilates' },
  { title: 'Smart Tech', handle: 'smart-tech' },
  { title: 'Återhämtning & Hälsa', handle: 'recovery-health' },
  { title: 'Kost & Hydrering', handle: 'kost-hydrering' },
  { title: 'Utrustning & Tillbehör', handle: 'utrustning-tillbehor' },
  { title: 'Sportlivsstil', handle: 'sportlivsstil' },

  // ── SMART TECH ──────────────────────────
  { title: 'Smartklockor', handle: 'smartklockor' },
  { title: 'Aktivitetsarmband', handle: 'aktivitetsarmband' },
  { title: 'Pulsmätare', handle: 'pulsmatare' },
  { title: 'Sporthörlurar', handle: 'sporthorlurar' },
  { title: 'Cykeldatorer', handle: 'cykeldatorer' },
  { title: 'Actionkameror', handle: 'actionkameror' },
  { title: 'GPS & Navigation', handle: 'gps-navigation' },
  { title: 'Belysning', handle: 'belysning' },
  { title: 'Laddning & Powerbanks', handle: 'powerbanks' },

  // ── LÖPNING ─────────────────────────────
  { title: 'Löparskor', handle: 'loparskor' },
  { title: 'Trail Running', handle: 'trail-running' },
  { title: 'Löparkläder', handle: 'loparklader' },
  { title: 'Hydrering för löpning', handle: 'lopning-hydrering' },
  { title: 'Löpartillbehör', handle: 'lopning-tillbehor' },

  // ── FITNESS ─────────────────────────────
  { title: 'Hemmagym', handle: 'hemmagym' },
  { title: 'Styrketräning', handle: 'styrketraning' },
  { title: 'Funktionell Träning', handle: 'funktionell-traning' },
  { title: 'Kondition', handle: 'kondition' },
  { title: 'Mobility & Stretch', handle: 'mobility-stretch' },

  // ── OUTDOOR ─────────────────────────────
  { title: 'Ryggsäckar', handle: 'ryggsackar' },
  { title: 'Tält', handle: 'talt' },
  { title: 'Sovutrustning', handle: 'sovutrustning' },
  { title: 'Campingkök', handle: 'campingkok' },
  { title: 'Vattenrening', handle: 'vattenrening' },
  { title: 'Friluftsverktyg', handle: 'friluftsverktyg' },

  // ── CYKLING ─────────────────────────────
  { title: 'Cykelhjälmar', handle: 'cykelhjalmar' },
  { title: 'Cykelkläder', handle: 'cykelklader' },
  { title: 'Cykelbelysning', handle: 'cykelbelysning' },
  { title: 'Cykelväskor', handle: 'cykelvaskor' },
  { title: 'Cykeltillbehör', handle: 'cykeltillbehor' },

  // ── YOGA ────────────────────────────────
  { title: 'Yogamattor', handle: 'yogamattor' },
  { title: 'Yogablock', handle: 'yogablock' },
  { title: 'Pilates', handle: 'pilates' },
  { title: 'Meditation', handle: 'meditation' },

  // ── RECOVERY ────────────────────────────
  { title: 'Massage', handle: 'massage' },
  { title: 'Kompression', handle: 'kompression' },
  { title: 'Stöd & Skydd', handle: 'stod-skydd' },
  { title: 'Stretching', handle: 'stretching' },
  { title: 'Kyl & Värme', handle: 'kyl-varme' },

  // ── KLÄDER ──────────────────────────────
  { title: 'Funktionskläder Herr', handle: 'herr-funktionsklader' },
  { title: 'Funktionskläder Dam', handle: 'dam-funktionsklader' },
  { title: 'Kompressionskläder Herr', handle: 'herr-kompression' },
  { title: 'Kompressionskläder Dam', handle: 'dam-kompression' },
  { title: 'Sport-BH', handle: 'sport-bh' },

  // ── SKOR ────────────────────────────────
  { title: 'Träningsskor', handle: 'traningsskor' },
  { title: 'Löparskor', handle: 'loparskor' },
  { title: 'Trailskor', handle: 'trailskor' },
  { title: 'Vandringskängor', handle: 'vandringskangor' },
  { title: 'Sandaler', handle: 'sandaler' },
];
  const results = { created: [], existing: [], failed: [] };
  const delay = ms => new Promise(r => setTimeout(r, ms));

  for (const col of COLLECTIONS) {
    try {
      const checkRes = await axios.get(
        `https://${domain}/admin/api/2024-01/custom_collections.json?handle=${col.handle}`,
        { headers: { 'X-Shopify-Access-Token': token } }
      );
      if (checkRes.data.custom_collections?.length > 0) {
        results.existing.push(col.title);
      } else {
        await axios.post(
          `https://${domain}/admin/api/2024-01/custom_collections.json`,
          { custom_collection: { title: col.title, handle: col.handle, published: true, sort_order: col.sort || 'best-selling' } },
          { headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' } }
        );
        results.created.push(col.title);
        console.log('✓ Created collection:', col.title);
      }
      await delay(300);
    } catch(e) {
      results.failed.push({ title: col.title, error: e.message });
      console.error('Collection failed:', col.title, e.message);
    }
  }

  res.json({
    ok: true,
    created: results.created.length,
    existing: results.existing.length,
    failed: results.failed.length,
    details: results
  });
});

// ── CRON: Run every 6 hours ────────────────────────────────
cron.schedule('0 */12 * * *', () => {
  console.log('Cron: Starting scheduled research...');
  runProductResearch();
});

// ── CRON: Sync performance every hour ─────────────────────
cron.schedule('0 * * * *', () => {
  console.log('Cron: Syncing Shopify performance...');
  syncShopifyPerformance();
});

// ── START ──────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
app.listen(PORT, async () => {
  await initDB();
  // Mount tracking routes if available
  if (trackingRoutes && db) {
    app.use('/track', trackingRoutes(db));
    console.log('✓ Tracking routes mounted at /track');
  }
  // Temporarily disabled until scoring is migrated to PostgreSQL
/*
if (startScoreCron && db) {
  startScoreCron(db);
  console.log('✓ Score cron started');
}
*/
  console.log(`Mercury Backend running on port ${PORT}`);
  console.log('Shopify:', process.env.SHOPIFY_DOMAIN || 'NOT SET');
  console.log('OpenAI:', process.env.OPENAI_API_KEY ? 'SET' : 'NOT SET');
  console.log('RapidAPI:', process.env.RAPIDAPI_KEY ? 'SET' : 'NOT SET');
  console.log('CJ:', process.env.CJ_API_KEY ? 'API Key SET' : (process.env.CJ_EMAIL || 'NOT SET'));
  console.log('Make.com:', process.env.MAKE_WEBHOOK_URL ? 'SET' : 'NOT SET');
  console.log('Email:', process.env.RESEND_API_KEY ? 'SET' : 'NOT SET');
  console.log('Orbit:', process.env.ORBIT_API_URL ? 'SET' : 'NOT SET');
  // Sync performance on startup
  setTimeout(syncShopifyPerformance, 10000);
  // Run first research on startup
  setTimeout(runProductResearch, 5000);
});
