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
// ── OUTDOOR-FIRST KEYWORD LISTS ────────────────────────────
// These keywords ALWAYS map to Friluftsliv & Outdoor — never to Träning & Fitness.
// Checked BEFORE the general CATEGORY_MAP to prevent misclassification.
const OUTDOOR_PRIORITY_KEYWORDS = [
  // Tält & Shelter
  'tent', 'tents', 'camping tent', 'dome tent', 'backpacking tent', 'shelter', 'tarp', 'bivvy', 'hammock tent',
  // Sovsäck & Isolering
  'sleeping bag', 'sleep bag', 'mummy bag', 'down sleeping', 'quilt sleeping',
  // Vandringsutrustning
  'trekking poles', 'trekking pole', 'hiking poles', 'hiking pole', 'walking poles', 'trail poles',
  'hiking boots', 'trail boots', 'approach shoes', 'hiking shoes', 'waterproof boots',
  // Ryggsäck outdoor
  'hiking backpack', 'trekking backpack', 'trail backpack', 'outdoor backpack', 'camping backpack',
  'daypack', 'summit pack',
  // Mat & Tillagning utomhus
  'camping stove', 'camp stove', 'backpacking stove', 'camping cookware', 'camp cookset',
  'camping pot', 'titanium pot', 'mess kit',
  // Belysning outdoor
  'headlamp', 'head lamp', 'camping lantern', 'camp light',
  // Navigation & Säkerhet
  'compass', 'orienteering', 'survival kit', 'emergency blanket', 'fire starter', 'flint',
  'carabiner', 'climbing rope', 'harness', 'belay',
  // Vattenrening
  'water filter', 'water purifier', 'water purification', 'life straw',
  // Övrigt friluftsliv
  'camping', 'camp gear', 'campsite', 'campfire', 'outdoor cooking',
  'hiking', 'trekking', 'trail running', 'trail run', 'mountaineering',
  'backpacking', 'bikepacking', 'kayaking', 'canoe', 'paddling',
  'rock climbing', 'bouldering', 'rappelling',
  'snowshoeing', 'snowshoe', 'ski touring', 'backcountry',
];

// ── KOLLEKTIONSREGLER ─────────────────────────────────────────
function mapToCollections(title, category, description) {
  const text = (title + ' ' + category + ' ' + (description||'')).toLowerCase();
  const raw  = title.toLowerCase();
  const cols = [];

  // 1. SMART TECH
  const techKW = ['smartwatch','gps watch','fitness tracker','activity tracker',
    'heart rate monitor','pulsmätare','bone conduction','cycling computer',
    'action camera','actionkamera','running watch','triathlon watch','smart ring',
    'fitness watch','sport watch','bluetooth headphone','wireless earbud',
    'sports earbud','smartglasögon','smart glasses','ar glasses','ai glasses',
    'open ear headphone','silikonarmband','fitnessklocka','gps klocka',
    'sporthörlurar','drönare','drone'];
  const isTech = techKW.some(k => text.includes(k));
  if (isTech) cols.push('smart-tech');

  // 2. OUTDOOR — utrustning, INTE kläder eller skor
  const outdoorEquipKW = ['tent','tält','sleeping bag','sovsäck','sovdyna',
    'sleeping pad','trekking pole','vandringsstav','hiking pole','camping stove',
    'campingkök','camping lantern','campinglampa','headlamp','pannlampa',
    'water filter','vattenfilter','carabiner','survival kit','emergency blanket',
    'kayak','paddling'];
  const hikingShoeKW = ['hiking boot','vandringskänga','vandringsskor',
    'trekking boot','klätterskor'];
  const isHikingShoe = hikingShoeKW.some(k => text.includes(k));
  const isOutdoorPack = (
    (text.includes('ryggsäck') || text.includes('backpack')) &&
    ['outdoor','vandring','hiking','trekking','camping'].some(k => text.includes(k))
  );
  const isOutdoor = (outdoorEquipKW.some(k => text.includes(k)) || isOutdoorPack || isHikingShoe) && !isTech;
  if (isOutdoor) cols.push('outdoor');

  // 3. LÖPNING — skor och accessoarer för löpning
  const runKW = ['running shoe','löparsko','löparskor','trail running shoe',
    'road running shoe','running jacket','löparjacka','running tights',
    'löpartights','running shorts','löparshorts','hydration vest','running belt',
    'löparbälte','löparmidjeväska','running cap','running sock','löparstrumpa',
    'kolfiberplatta','löparskor'];
  const isRunning = (runKW.some(k => text.includes(k)) ||
    (text.includes('löpning') && !text.includes('pannlampa')))
    && !isHikingShoe && !isTech;
  if (isRunning) cols.push('lopning');

  // 4. YOGA & WELLNESS — mattor, block, yogakläder
  const yogaKW = ['yoga','pilates','meditation','yogamatta','yoga mat',
    'yoga block','yoga strap','yogablock','mindfulness','bolster'];
  if (yogaKW.some(k => text.includes(k))) cols.push('yoga-wellness');

  // 5. RECOVERY — massage, stöd, kompression
  const recoveryKW = ['massage gun','massagepistol','foam roller','skumrulle',
    'massage ball','knee brace','knästöd','ankle brace','ankelbandage',
    'elbow brace','compression sleeve','ice bath','isbad','kinesio tape',
    'sports tape','back stretcher','axelvärmare','massageplatta','vibration plate',
    'massagepaket','andningsträna','breathing trainer','lungkapacitet',
    'nackstöd','neck support','ryggstöd','back support','lumbar',
    'smärtlindring','pain relief'];
  const isRecovery = recoveryKW.some(k => text.includes(k)) && !isTech;
  if (isRecovery) cols.push('recovery');

  // 6. NUTRITION — flaskor, shakers, kosttillskott (för människor)
  const nutritionKW = ['protein shaker','proteinshaker','water bottle',
    'vattenflaska','hydration bottle','shaker bottle','electrolyte','energy gel',
    'whey protein','vassleprotein','protein powder','proteinpulver','pre workout',
    'bcaa','creatine','kreatin','sports drink','sportdryck','meal prep',
    'träningsflaska'];
  if (nutritionKW.some(k => text.includes(k))) cols.push('nutrition');

  // 7. TRÄNING — BARA redskap och utrustning, INTE kläder
  const traningEquipKW = ['resistance band','träningsband','pull up bar','ab roller',
    'jump rope','hopprepet','kettlebell','dumbbell','hantel','barbell','skivstång',
    'gym gloves','lifting belt','lyftbälte','weightlifting belt','crossfit',
    'battle rope','push up handles','armtränare','arm trainer','mini cykel',
    'träningscykel','spinning','stepmaskin','stepper','roddmaskin','gymväska',
    'gym bag','träningsutrustning','gym equipment','punching bag','boxningssäck'];
  const isTraningEquip = traningEquipKW.some(k => text.includes(k)) &&
    !isOutdoor && !isHikingShoe && !isRunning && !isTech;
  if (isTraningEquip) cols.push('traning');

  // 8. KÖN — kläder och skor hamnar under herr/dam
  const damKW = [' dam',' för dam','dam ','women',' hennes','för henne',
    'damskor','damjacka','sport-bh','sportbh','tränings-bh','dam shorts',
    'damtröja','dam leggings'];
  const herrKW = [' herr','för herr','herr ','herrskjorta','herrtröja',
    'herrbyxor','herrkänga','herrskor','för män','för herrar','herrstickad',
    'herrtopp','herr shorts','herr hoodie'];
  const isDam = damKW.some(k => raw.includes(k));
  const isHerr = herrKW.some(k => raw.includes(k));
  if (isDam) cols.push('dam');
  if (isHerr) cols.push('herr');

  // Kläder utan könsmärkning → både herr och dam (eller träning om sport)
  const isClothing = ['jacket','jacka','tröja','shirt','shorts','byxor',
    'leggings','tights','linne','hoodie','fleece','träningskläder',
    'sportkläder','atletisk'].some(k => text.includes(k));

  // Om det är sportkläder utan kön → lägg i träning
  const isSportClothing = isClothing && 
    ['sport','träning','gym','athletic','performance','compression',
     'dry fit','moisture'].some(k => text.includes(k)) &&
    !isDam && !isHerr;
  if (isSportClothing && !isRunning && !isOutdoor && !yogaKW.some(k=>text.includes(k))) {
    cols.push('traning');
  }

  // Fallback
  if (cols.length === 0) {
    if (isClothing) cols.push('traning');
    else cols.push('traning');
  }

  return [...new Set(cols)];
}



function mapCategory(rawCategory, productTitle) {
  const handles = mapToCollections(productTitle, rawCategory, '');
  const primary = handles[0];
  const MAP = {
    'traning':       { sv: 'Träning & Fitness', tag: 'gym',        gender: 'unisex' },
    'lopning':       { sv: 'Löpning',           tag: 'löpning',    gender: 'unisex' },
    'outdoor':       { sv: 'Outdoor',           tag: 'outdoor',    gender: 'unisex' },
    'yoga-wellness': { sv: 'Yoga & Wellness',   tag: 'yoga',       gender: 'dam'    },
    'recovery':      { sv: 'Återhämtning',      tag: 'recovery',   gender: 'unisex' },
    'nutrition':     { sv: 'Nutrition',         tag: 'nutrition',  gender: 'unisex' },
    'smart-tech':    { sv: 'Smart Teknik',      tag: 'smart-tech', gender: 'unisex' },
    'herr':          { sv: 'Herr',              tag: 'herr',       gender: 'herr'   },
    'dam':           { sv: 'Dam',               tag: 'dam',        gender: 'dam'    },
  };
  return { ...(MAP[primary] || MAP['traning']), shopify: MAP[primary]?.sv || 'Träning & Fitness', handles };
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
// ── PRODUKTFILTER — WHITELIST-FIRST ──────────────────────
// Principen: produkten MÅSTE aktivt bevisa att den hör till
// sport / träning / friluftsliv / återhämtning / aktiv hälsa.
// Allt annat blockeras — oavsett hur titeln är formulerad.

// ── SPORT WHITELIST — måste matcha minst ett ──────────────
const SPORT_WHITELIST = [
  // Träningsutrustning
  'resistance band','träningsband','motståndsband',
  'pull up','chin up','dip bar','ab roller','sit up',
  'jump rope','hopprepet','battle rope',
  'kettlebell','dumbbell','hantel','barbell','skivstång','weight plate',
  'gym gloves','träningshandskar','lifting belt','lyftbälte',
  'push up','push-up','liggstöd',
  'punching bag','boxningssäck','boxing glove','boxningshandske',
  'speed bag','fokusvantar',
  'rowing machine','roddmaskin','rowing',
  'exercise bike','träningscykel','spinning bike','motionscykel',
  'stepper','stepmaskin','elliptical','cross trainer',
  'mini bike','mini cykel','pedal exerciser',
  'pull rope','armtränare','arm trainer','chest expander',
  'agility ladder','agility cone','sport cone',
  'weighted vest','viktväst','ankle weight','handled weight',
  'gym bag','gymväska','sports bag','duffel bag sport',
  'gym towel','sport towel','microfiber towel sport',
  'sport water bottle','vattenflaska sport','protein shaker','proteinshaker',
  'shaker bottle','blender bottle',

  // Löpning
  'running shoe','löparsko','löparskor','trail running','road running',
  'running jacket','löparjacka','running tights','löpartights',
  'running shorts','löparshorts','running vest','löparväst',
  'hydration vest','running belt','löparbälte','löparmidjeväska',
  'running cap','running sock','löparstrumpa','running headband',
  'carbon plate','kolfiberplatta','marathon shoe',
  'trail shoe','trailsko',

  // Sportkläder — aktivt kopplade till sport
  'compression shirt','compression tights','compression leggings',
  'compression shorts','base layer','thermal running',
  'sport bra','sports bra','sport-bh','sportbh','tränings-bh',
  'gym shirt','gym top','gym shorts','gym leggings','gym pants',
  'training shorts','training tights','training jacket','training top',
  'athletic shorts','athletic top','athletic leggings',
  'workout shorts','workout top','workout leggings',
  'dry fit','dri-fit','moisture wicking','quick dry sport',
  'cycling jersey','cycling shorts','cycling tight','cycling jacket',
  'cycling gloves','cycling shoe','cycling helmet',
  'swimsuit','swim shorts','wetsuit','rash guard',
  'ski jacket','ski pants','ski gloves','ski helmet','ski goggles',
  'snowboard jacket','snowboard pants',

  // Skor — aktivt sportkopplade
  'training shoe','träningsskor','gym shoe','crossfit shoe',
  'hiking boot','vandringskänga','vandringsskor','trekking boot',
  'approach shoe','climbing shoe','klätterskor',
  'cycling shoe','cykelsko',
  'football boot','soccer cleat','rugby boot',

  // Outdoor & Friluftsliv
  'camping tent','tält','tent','backpacking tent',
  'sleeping bag','sovsäck','sleeping pad','sovdyna','sleeping mat',
  'trekking pole','vandringsstav','hiking pole','walking pole',
  'hiking backpack','vandringryggsäck','trekking backpack',
  'outdoor backpack','camping backpack','daypack',
  'camping stove','camp stove','camping cookware','camping pot',
  'camping lantern','headlamp','pannlampa','led headlamp',
  'water filter','water purifier','vattenfilter',
  'carabiner','climbing harness','climbing rope','belay device',
  'survival kit','emergency blanket','fire starter',
  'kayak paddle','paddling','canoe','kayak',
  'bike helmet','cykelhjälm',
  'bike light','cykellampa','cycling computer','cykeldator',
  'bike bag','cykelväska','saddle bag',

  // Yoga & Wellness
  'yoga mat','yogamatta','yoga block','yogablock',
  'yoga strap','yoga wheel','yoga towel',
  'pilates ring','pilates mat','pilates sock',
  'meditation cushion','meditation pillow','meditationsdyna',
  'stretching strap','foam wedge yoga',

  // Återhämtning
  'massage gun','massagepistol','percussion massager',
  'foam roller','skumrulle','massage roller',
  'massage ball','lacrosse ball','trigger point',
  'knee brace','knästöd','knee sleeve','knee support',
  'ankle brace','ankle support','ankelbandage',
  'elbow brace','wrist brace','handledsband',
  'compression sleeve','calf sleeve','shin sleeve',
  'ice bath','isbad','cold therapy','cold compress',
  'heat therapy','heat pad','värmekudde sport',
  'kinesio tape','sports tape','athletic tape',
  'back stretcher','ryggstretchare','posture corrector',
  'acupressure mat','nail mat',
  'back brace','lumbar support','ryggstöd',
  'neck traction','neck stretcher','nacksträckare',
  'recovery sandal','recovery slide',
  'breathing trainer','andningsträna','lung trainer',
  'vibration plate','massageplatta','vibration board',
  'shoulder massager','axelvärmare','neck massager','nackmassor',

  // Smart Tech för aktiv livsstil
  'smartwatch','smart watch','gps watch','running watch',
  'fitness tracker','activity tracker','fitness band',
  'heart rate monitor','pulsmätare','chest strap',
  'sports earbuds','sporthörlurar','bone conduction',
  'wireless sport earbuds','waterproof earbuds',
  'open ear headphones','sport headphones',
  'action camera','actionkamera','sports camera','helmet camera',
  'cycling computer','cykeldator','bike computer',
  'gps navigation outdoor','outdoor gps',
  'smart ring','fitness ring',
  'triathlon watch','multisport watch',
  'cadence sensor','speed sensor bike',
  'power meter cycling','bike radar',
  'solar charger outdoor','camping power bank',
  'sport sunglasses','löparglasögon',

  // Nutrition & Hydrering
  'electrolyte','elektrolyt','energy gel','energigel',
  'energy bar sport','sportbar','protein bar',
  'whey protein','vassleprotein','protein powder','proteinpulver',
  'pre workout','bcaa','creatine','kreatin',
  'sports drink','sportdryck','isotonic',
  'hydration pack','camelbak','water bladder',
  'insulated water bottle','termosflaska sport',
  'meal prep container sport',

  // Sport-accessoarer
  'swim goggle','simglasögon','swim cap','simkeps','swimming fin',
  'snorkel','diving mask','dykmask',
  'golf glove','golf bag','golf ball','putting mat',
  'tennis racket','squash racket','badminton racket',
  'sport belt','running belt','waist pack running',
  'reflective vest running','reflective jacket running',
  'phone arm band running','phone holder running',
  'sport watch band','klockband sport',
];

// ── HARD BLOCK — dessa stoppas alltid oavsett whitelist ──
const HARD_BLOCKED = [
  // Bilar & fordon
  'takräcke','roof rack','roof bar','car roof','crossbar','roof rail',
  'car seat','car floor mat','car organizer','car charger','dash cam',
  'windshield','steering wheel','obd','auto part','vehicle',
  'motorcycle','moped','scooter part',

  // Husdjur
  'för katt','för hunden','för hundar','för husdjur','för djur',
  'pet food','pet treat','cat food','dog food','bird food',
  'cat toy','dog toy','pet collar','pet leash','pet carrier',
  'pet supplement','pet vitamin','taurine för',
  'aquarium','fish tank','bird cage','hamster',
  'husdjur','sällskapsdjur','djurvård',

  // Hem & Inredning
  'home decor','home decoration','wall art','curtain','tablecloth',
  'bedding','duvet','pillow case','mattress','sofa','couch',
  'garden lamp','solar garden','planter','flower pot',
  'mosquito killer','bug zapper','fly trap',
  'shower curtain','bath mat','toilet','bathroom cleaner',
  'kitchen knife','cutting board','cooking pot','frying pan',
  'air fryer','coffee maker','blender','food processor',

  // Skönhet & Hudvård
  'skincare','skin care','face mask','face cream','serum',
  'anti-aging','wrinkle','cleanser','toner','moisturizer',
  'makeup','cosmetic','lipstick','mascara','foundation',
  'nail polish','eyelash','hair mask','hair serum','hair oil',
  'hair removal','epilator','body scrub','exfoliant',

  // Mode & Vanliga kläder
  'wedding dress','evening dress','cocktail dress','party dress',
  'tuxedo','high heel','stiletto','fashion sneaker','casual sneaker',
  'casual wear','streetwear','daily wear','fashion ring',
  'necklace','earring','bracelet','jewelry','anklet',
  'hair clip','scrunchie','hårsnodd',
  'polo shirt fashion','knit sweater fashion',
  'denim jacket','jeans','chinos',

  // Baby & Barn (leksaker)
  'baby bottle','baby diaper','pacifier','baby monitor',
  'toy ','leksak','children toy','kids toy',
  'trampoline','swing set','sandbox','slide',

  // Övrigt
  'gambling','casino','drug','weapon','firearm',
  'cigarette','tobacco','vape','alcohol','whiskey',
  'fake','replica','counterfeit',
  'gaming keyboard','gaming mouse','gaming chair',
  'office chair','desk lamp','laptop stand',
  'led strip','smart bulb','smart plug',
  'suitcase','luggage','travel bag fashion',
  'picnic blanket','pool float','inflatable pool',
];

function isProductBlocked(product) {
  const title   = (product.title || product.nameEn || product.name || '').toLowerCase();
  const rawTitle = (product.rawTitle || product.nameEn || '').toLowerCase();
  const cat     = (product.categoryName || product.category || '').toLowerCase();
  const desc    = (product.description || '').toLowerCase();
  const fullText = title + ' ' + rawTitle + ' ' + cat + ' ' + desc;
  const keyword  = (product.keyword || '').toLowerCase();

  // ── STEG 1: Hard block — dessa stoppas alltid ──────────
  for (const kw of HARD_BLOCKED) {
    if (fullText.includes(kw)) {
      console.log('Blocked: ' + kw + ' in: ' + title);
      return true;
    }
  }

  // ── STEG 2: Titeln för kort ────────────────────────────
  if (title.length < 8) {
    console.log('Title too short: ' + title);
    return true;
  }

  // ── STEG 3: Whitelist — måste matcha minst ett ─────────
  // Kollar titel + råtitel + sökordet som triggade sökningen
  const checkText = title + ' ' + rawTitle + ' ' + keyword;
  const passesWhitelist = SPORT_WHITELIST.some(kw => checkText.includes(kw));

  if (!passesWhitelist) {
    console.log('Not sport: ' + title + ' raw: ' + rawTitle.slice(0,40));
    return true;
  }

  return false;
}

// Auto-publish threshold
const AUTO_PUBLISH_SCORE = 70; // >=70 → direkt till Meloni utan granskning
const MIN_SCORE = 60;          // Products below 60 are rejected
const MAX_SHIPPING_DAYS = 21;  // Max shipping days — only used when data is available

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
    const pageNum = Math.floor(Math.random() * 4) + 1;
    const orderBy = ['ORDER_COUNT','PRICE','COMMENTS'][Math.floor(Math.random() * 3)];
    const res = await axios.get('https://developers.cjdropshipping.com/api2.0/v1/product/list', {
      headers: { 'CJ-Access-Token': token },
      params: { productNameEn: keyword, pageNum, pageSize: limit, orderBy, orderType: 'DESC' }
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
  let score = 50; // Base score — CJ products are pre-vetted
  const price = parseFloat(product.sellPrice) || 10;
  if (price >= 1 && price <= 100) score += 15;
  const orders = parseInt(product.orderCount) || 0;
  score += Math.min(20, orders / 50);
  const imgs = (product.productImageSet || []).length;
  score += Math.min(10, imgs * 2);
  score += Math.max(0, 10 - index);
  return Math.min(100, Math.round(score));
}

// ── SHOPIFY API ────────────────────────────────────────────
// ── STORLEKSDETEKTERING ────────────────────────────────────
// Returns a Shopify-formatted variants array with sizes, or single default variant.
function buildVariants(product) {
  const basePrice  = (product.sellPrice || 199).toString();
  const baseSku    = `VIN-${product.aliId || product.cjPid || 'AUTO'}`;
  const baseWeight = product.weight || 0.3;
  const baseStock  = product.stock  || 50;

  // Standard size sets — largest match wins
  const SIZE_SETS = {
    clothing: ['XS', 'S', 'M', 'L', 'XL', 'XXL'],
    shoes:    ['36', '37', '38', '39', '40', '41', '42', '43', '44', '45'],
    kids:     ['90', '100', '110', '120', '130', '140'],
    onesize:  null  // sentinel: no variants
  };

  // Keywords that indicate clothing sizes
  const CLOTHING_KW = [
    'shirt', 'tee', 't-shirt', 'hoodie', 'jacket', 'vest', 'shorts', 'pants', 'leggings',
    'tights', 'jersey', 'top', 'bra', 'sports bra', 'swimsuit', 'wetsuit', 'compression',
    'tracksuit', 'sweatshirt', 'pullover', 'fleece', 'base layer', 'thermal', 'gloves',
    'socks', 'beanie', 'hat', 'cap', 'buff', 'neck gaiter',
    'träningströja', 'träningsbyxor', 'löparbyxor', 'löpartröja',
  ];

  // Keywords that indicate shoe sizes
  const SHOE_KW = [
    'shoes', 'boots', 'sneakers', 'trainers', 'runners', 'cleats', 'sandals',
    'hiking boots', 'trail shoes', 'running shoes', 'gym shoes', 'cycling shoes',
    'skor', 'stövlar', 'löparskor', 'vandringsskor',
  ];

  // Keywords that clearly indicate one-size / non-apparel
  const ONE_SIZE_KW = [
    'mat', 'roller', 'band', 'bottle', 'bag', 'backpack', 'watch', 'tracker',
    'earbuds', 'headphones', 'camera', 'rope', 'kettle', 'weight', 'dumbbell',
    'barbell', 'pole', 'tent', 'sleeping bag', 'stove', 'lamp', 'lantern',
  ];

  const titleLower = (product.title || product.rawTitle || '').toLowerCase();
  const catLower   = (product.category || '').toLowerCase();
  const text       = titleLower + ' ' + catLower;

  // Check CJ variant data first (most reliable)
  if (product.cjVariants && Array.isArray(product.cjVariants) && product.cjVariants.length > 1) {
    return product.cjVariants.map((v, i) => ({
      option1: v.variantName || v.name || `Variant ${i + 1}`,
      price: basePrice,
      sku: `${baseSku}-${(v.variantName || i).toString().replace(/\s+/g, '-').toUpperCase()}`,
      inventory_management: 'shopify',
      inventory_quantity: v.variantStock || baseStock,
      weight: baseWeight,
      weight_unit: 'kg'
    }));
  }

  // One-size check
  if (ONE_SIZE_KW.some(kw => text.includes(kw))) {
    return [buildDefaultVariant(basePrice, baseSku, baseWeight, baseStock)];
  }

  // Shoe check
  if (SHOE_KW.some(kw => text.includes(kw))) {
    return SIZE_SETS.shoes.map(size => ({
      option1: size,
      price: basePrice,
      sku: `${baseSku}-${size}`,
      inventory_management: 'shopify',
      inventory_quantity: baseStock,
      weight: baseWeight,
      weight_unit: 'kg'
    }));
  }

  // Kids check
  if (text.includes('kids') || text.includes('children') || text.includes('child') || text.includes('barn')) {
    return SIZE_SETS.kids.map(size => ({
      option1: size,
      price: basePrice,
      sku: `${baseSku}-${size}`,
      inventory_management: 'shopify',
      inventory_quantity: baseStock,
      weight: baseWeight,
      weight_unit: 'kg'
    }));
  }

  // Clothing check
  if (CLOTHING_KW.some(kw => text.includes(kw))) {
    return SIZE_SETS.clothing.map(size => ({
      option1: size,
      price: basePrice,
      sku: `${baseSku}-${size}`,
      inventory_management: 'shopify',
      inventory_quantity: baseStock,
      weight: baseWeight,
      weight_unit: 'kg'
    }));
  }

  // Default: single variant
  return [buildDefaultVariant(basePrice, baseSku, baseWeight, baseStock)];
}

function buildDefaultVariant(price, sku, weight, stock) {
  return {
    price,
    sku,
    inventory_management: 'shopify',
    inventory_quantity: stock,
    weight,
    weight_unit: 'kg'
  };
}

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
      published: true,
      published_at: new Date().toISOString(),
      published_scope: 'web',
      variants: buildVariants(product),
      options: (() => {
        const vars = buildVariants(product);
        // Only add "Size" option when we actually have size variants
        if (vars.length > 1 && vars[0].option1) {
          return [{ name: 'Storlek', values: vars.map(v => v.option1) }];
        }
        return undefined;
      })(),
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
  const shopifyProduct = res.data.product;
  // Assign to correct collections
  const handles = mapToCollections(product.title, product.category, product.description);
  for (const handle of handles) {
    try { await addProductToCollection(shopifyProduct.id, handle); } catch(e) {}
  }
  return shopifyProduct;
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
Fråga: [Den vanligaste frågan en svensk kund ställer om den här produkten]
Svar: [Kort, ärligt svar på 1-2 meningar. Inga löften du inte kan hålla.]
Fråga: [En fråga om leverans, storlek eller passform]
Svar: [Konkret svar — t.ex. "Finns i XS–XL, se storleksguide. Leverans 7–14 dagar."]
Fråga: [En fråga om kvalitet, material eller hållbarhet]
Svar: [Ärligt svar som bygger förtroende utan att överdriva.]

VIKTIGT: Skriv ALLTID alla tre Fråga/Svar-par. Lämna aldrig ett Svar tomt.

AD_HOOK: [En rad för Instagram/TikTok — max 10 ord. Ska kännas äkta, inte som reklam. T.ex. "Därför har alla börjat använda den här" eller "Äntligen slipper du det här problemet"]

TAGS: [5 taggar, gemener, relevanta, på svenska]

Viktigt: Inga engelska ord om det finns ett bra svenskt alternativ. Inget "dropshipping", inga leverantörsnamn, inga påhittade specifikationer.`;

  const res = await openai.chat.completions.create({
    model: 'gpt-4o',
    max_tokens: 1200,
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

  // Render FAQ: convert "Fråga: ... Svar: ..." pairs into proper HTML
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
  // Träning & Fitness
  'resistance bands', 'massage gun', 'foam roller', 'pull up bar',
  'jump rope', 'gym gloves', 'weightlifting belt', 'ab roller',
  'fitness tracker', 'smartwatch sport', 'heart rate monitor',
  'yoga mat', 'yoga block', 'workout leggings', 'sports bra high support',
  'compression socks', 'gym bag', 'water bottle insulated',
  'protein shaker', 'gym shoes men', 'running shoes women',
  // Friluftsliv & Outdoor
  'hiking boots', 'trekking poles', 'camping headlamp',
  'sleeping bag', 'outdoor backpack', 'waterproof jacket',
  'trail running shoes', 'cycling gloves', 'bike helmet',
  'carabiner', 'camping stove', 'water filter outdoor',
  // Återhämtning & Hälsa
  'knee brace support', 'back stretcher', 'posture corrector',
  'acupressure mat', 'ice pack recovery', 'compression sleeve',
  'ankle support', 'elbow brace', 'muscle roller stick',
  'sleep mask', 'eye mask heated', 'neck massager',
  // Smart Teknik sport
  'running headphones', 'sports earbuds wireless', 'gps watch',
  'cycling computer', 'action camera', 'solar charger outdoor',
  // Kost & Vätska  
  'meal prep container', 'supplement organizer', 'blender bottle',
  'electrolyte powder', 'energy gel', 'hydration vest',
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
    const keywords = overrideKeywords ? overrideKeywords : shuffle(TREND_KEYWORDS).slice(0, 3);
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
        const cjKeywords = shuffle(TREND_KEYWORDS).slice(0, 3);
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
    const top = candidates.slice(0, 20);

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
      if (idExists) continue;

      // Skip if very similar title already in queue (duplicate from different source)
      const productTitle = (product.title||product.nameEn||'').toLowerCase().slice(0,30);
      const titleExists = productTitle.length > 5 && [...store.queue, ...store.products].find(
        p => p.rawTitle?.toLowerCase().slice(0,30) === productTitle
      );
      if (titleExists) continue;

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


// Research a specific keyword
app.post('/api/research/keyword', async (req, res) => {
  const { keyword } = req.body;
  if (!keyword) return res.status(400).json({ error: 'keyword required' });
  res.json({ ok: true, message: `Researching: ${keyword}` });
  runProductResearch([keyword]);
});

// Bulk seed — runs through a curated list covering all main categories.
// Pass { categories: ["running","yoga",...] } to target specific ones,
// or omit body to seed everything. delay_ms between each keyword (default 8000).
app.post('/api/seed', async (req, res) => {
  const { categories, delay_ms = 8000 } = req.body || {};

  const SEED_KEYWORDS = {
    // ── HERR ─────────────────────────────────────────────────
    'herr-tshirts':       ['men sport t-shirt gym', 'men tank top training', 'men gym shirt dry fit'],
    'herr-hoodies':       ['men hoodie sport', 'men sweatshirt training', 'men pullover fleece sport'],
    'herr-funktion':      ['men compression shirt long sleeve', 'men base layer top', 'men running shirt lightweight'],
    'herr-kompression':   ['men compression tights', 'men compression leggings sport', 'men compression pants running'],
    'herr-shorts':        ['men gym shorts', 'men running shorts', 'men training shorts 2in1'],
    'herr-byxor':         ['men joggers sport', 'men track pants training', 'men sweatpants tapered'],
    'herr-understall':    ['men thermal underwear set', 'men base layer winter', 'men long johns sport'],
    'herr-jackor':        ['men windbreaker running jacket', 'men softshell jacket outdoor', 'men fleece jacket sport'],
    'herr-strumpor':      ['men running socks', 'men compression socks sport', 'men athletic socks ankle'],
    'herr-traningsskor':  ['men training shoes crossfit', 'men gym shoes', 'men cross trainer shoes'],
    'herr-loparskor':     ['men running shoes lightweight', 'men jogging shoes road'],
    'herr-trailskor':     ['men trail running shoes', 'men trail shoes grip'],
    'herr-vandring':      ['men hiking boots waterproof', 'men trekking boots ankle'],
    'herr-sandaler':      ['men sport sandals outdoor', 'men trekking sandals'],

    // ── DAM ──────────────────────────────────────────────────
    'dam-tshirts':        ['women sport t-shirt', 'women tank top gym', 'women workout top dry fit'],
    'dam-hoodies':        ['women hoodie sport', 'women sweatshirt zip', 'women pullover gym'],
    'dam-funktion':       ['women base layer top', 'women compression shirt sport', 'women long sleeve running'],
    'dam-kompression':    ['women compression tights yoga', 'women sport leggings high waist', 'women yoga pants seamless'],
    'dam-shorts':         ['women gym shorts', 'women biker shorts', 'women running shorts women'],
    'dam-byxor':          ['women joggers sport', 'women training pants', 'women track pants'],
    'dam-jackor':         ['women windbreaker jacket sport', 'women fleece jacket', 'women rain jacket running'],
    'dam-understall':     ['women thermal underwear', 'women base layer winter sport'],
    'sport-bh-latt':      ['sports bra low impact yoga', 'light support bra workout'],
    'sport-bh-medium':    ['sports bra medium support', 'medium impact sports bra training'],
    'sport-bh-hog':       ['sports bra high impact running', 'high support sports bra'],
    'dam-loparskor':      ['women running shoes', 'women jogging shoes lightweight'],
    'dam-traningsskor':   ['women training shoes gym', 'women crossfit shoes'],
    'dam-vandring':       ['women hiking boots waterproof', 'women trekking shoes'],

    // ── BARN ─────────────────────────────────────────────────
    'barn-tshirts':       ['kids sport t-shirt', 'children hoodie sport', 'youth gym shirt'],
    'barn-shorts':        ['kids sport shorts', 'children training pants', 'youth gym shorts'],
    'barn-jackor':        ['kids rain jacket outdoor', 'children windbreaker jacket'],
    'barn-outdoor':       ['kids outdoor clothing set', 'children hiking pants', 'youth fleece jacket'],
    'barn-understall':    ['kids thermal underwear', 'children base layer sport'],
    'barn-traningsskor':  ['kids training shoes', 'children gym shoes sport'],
    'barn-loparskor':     ['kids running shoes', 'children sport shoes running'],
    'barn-outdoorskor':   ['kids outdoor shoes', 'children hiking shoes'],
    'barn-vandring':      ['kids hiking boots', 'children trekking boots waterproof'],

    // ── TRÄNING & FITNESS ─────────────────────────────────────
    'fitness-styrka':     ['resistance bands set loop', 'pull up bar doorway', 'ab roller wheel core', 'push up handles'],
    'fitness-kondition':  ['jump rope speed crossfit', 'battle rope training', 'agility ladder sport'],
    'fitness-utrustning': ['weightlifting belt support', 'gym gloves grip', 'wrist wraps lifting', 'lifting straps gym'],
    'fitness-tillbehor':  ['gym bag men', 'gym bag women', 'gym towel microfiber', 'sport water bottle gym'],

    // ── FRILUFTSLIV & OUTDOOR ─────────────────────────────────
    'outdoor-talt':       ['camping tent 2 person lightweight', 'backpacking tent ultralight'],
    'outdoor-sovs':       ['sleeping bag 3 season', 'sleeping bag mummy lightweight'],
    'outdoor-ryggsack':   ['hiking backpack 30l', 'trekking backpack daypack', 'outdoor backpack waterproof'],
    'outdoor-nav':        ['trekking poles carbon', 'hiking poles adjustable', 'headlamp rechargeable camping'],
    'outdoor-kök':        ['camping stove portable', 'camping cookware set titanium', 'water filter hiking'],
    'outdoor-safety':     ['carabiner climbing', 'survival kit outdoor', 'emergency blanket camping'],

    // ── ÅTERHÄMTNING & HÄLSA ──────────────────────────────────
    'recovery-massage':   ['massage gun deep tissue', 'foam roller muscle recovery', 'massage ball trigger point'],
    'recovery-stöd':      ['knee brace support sport', 'ankle brace support', 'elbow brace tennis'],
    'recovery-stretch':   ['stretching strap flexibility', 'posture corrector back', 'acupressure mat neck'],

    // ── SMART TEKNIK ─────────────────────────────────────────
    'teknik-klockor':     ['fitness tracker watch sport', 'gps running watch', 'heart rate monitor chest'],
    'teknik-ljud':        ['wireless earbuds sport waterproof', 'bone conduction headphones running'],
    'teknik-gadgets':     ['action camera sport waterproof', 'cycling computer gps', 'phone arm band running'],

    // ── KOST & VÄTSKA ─────────────────────────────────────────
    'kost-flaskor':       ['insulated water bottle 1l', 'protein shaker bottle', 'hydration vest running'],
    'kost-nutrition':     ['electrolyte powder sport', 'energy gel running', 'meal prep container set'],

    // ── LÖPNING ───────────────────────────────────────────────
    'lopning-accessoarer':['running belt waist', 'running vest reflective', 'running headband', 'arm band phone running'],

    // ── VANDRING & CAMPING ────────────────────────────────────
    'vandring-accessoarer':['trekking poles ultralight', 'sleeping pad camping', 'camp lantern led', 'hiking water filter'],

    // ── CYKLING ───────────────────────────────────────────────
    'cykling-kläder':     ['cycling jersey men short sleeve', 'cycling shorts bib padded', 'cycling jacket windproof'],
    'cykling-accessoarer':['cycling gloves gel padded', 'bike light front rear set', 'bike bag frame', 'cycling helmet'],

    // ── YOGA ─────────────────────────────────────────────────
    'yoga-utrustning':    ['yoga mat non slip thick', 'yoga block cork set', 'yoga strap stretch', 'yoga wheel back'],
    'yoga-kläder':        ['yoga leggings women seamless', 'pilates socks grip', 'meditation cushion'],

    // ── LIVSSTIL ──────────────────────────────────────────────
    'livsstil':           ['active lifestyle bag', 'wellness sport accessories', 'sport lifestyle clothing'],
  };

  const selected = categories
    ? Object.entries(SEED_KEYWORDS).filter(([k]) => categories.includes(k))
    : Object.entries(SEED_KEYWORDS);

  const total = selected.reduce((n, [, kws]) => n + kws.length, 0);
  res.json({ ok: true, message: `Seeding ${total} keywords across ${selected.length} categories. This will take ~${Math.round(total * delay_ms / 60000)} minutes.`, total });

  // Run in background — staggered to avoid rate limits
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


// ── AKTIVERA ALLA UTKAST ──────────────────────────────────
app.post('/api/products/activate-all', async (req, res) => {
  const domain = process.env.SHOPIFY_DOMAIN;
  const token  = process.env.SHOP_TOKEN || process.env.SHOPIFY_TOKEN;
  if (!domain || !token) return res.status(500).json({ error: 'Shopify ej konfigurerat' });
  try {
    const r = await axios.get(
      `https://${domain}/admin/api/2024-01/products.json?status=draft&limit=250`,
      { headers: { 'X-Shopify-Access-Token': token } }
    );
    const drafts = r.data.products || [];
    res.json({ ok: true, message: `Aktiverar ${drafts.length} utkast...`, total: drafts.length });
    (async () => {
      const delay = ms => new Promise(r => setTimeout(r, ms));
      for (const p of drafts) {
        try {
          await axios.put(
            `https://${domain}/admin/api/2024-01/products/${p.id}.json`,
            { product: { id: p.id, status: 'active', published: true, published_at: new Date().toISOString() } },
            { headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' } }
          );
          await delay(300);
        } catch(e) {}
      }
      console.log('[ACTIVATE] Done');
    })();
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── RENSA + OMTILLDELA KOLLEKTIONER ───────────────────────
app.post('/api/products/fix-all', async (req, res) => {
  const domain = process.env.SHOPIFY_DOMAIN;
  const token  = process.env.SHOP_TOKEN || process.env.SHOPIFY_TOKEN;
  if (!domain || !token) return res.status(500).json({ error: 'Shopify ej konfigurerat' });
  const products = store.products.filter(p => p.shopifyId);
  res.json({ ok: true, message: `Omtilldelar ${products.length} produkter...`, total: products.length });
  (async () => {
    const delay = ms => new Promise(r => setTimeout(r, ms));
    // Get all collections to find IDs
    const colRes = await axios.get(
      `https://${domain}/admin/api/2024-01/custom_collections.json?limit=250`,
      { headers: { 'X-Shopify-Access-Token': token } }
    );
    const allCols = colRes.data.custom_collections || [];
    const colMap = {};
    allCols.forEach(c => { colMap[c.handle] = c.id; });

    // Remove all existing collection memberships for each product
    for (const item of products) {
      try {
        const collectsRes = await axios.get(
          `https://${domain}/admin/api/2024-01/collects.json?product_id=${item.shopifyId}`,
          { headers: { 'X-Shopify-Access-Token': token } }
        );
        const collects = collectsRes.data.collects || [];
        for (const c of collects) {
          await axios.delete(
            `https://${domain}/admin/api/2024-01/collects/${c.id}.json`,
            { headers: { 'X-Shopify-Access-Token': token } }
          );
          await delay(100);
        }
        // Re-assign to correct collections
        const handles = mapToCollections(item.title, item.category, item.description);
        for (const handle of handles) {
          const colId = colMap[handle];
          if (colId) {
            try {
              await axios.post(
                `https://${domain}/admin/api/2024-01/collects.json`,
                { collect: { product_id: item.shopifyId, collection_id: colId } },
                { headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' } }
              );
            } catch(e) {}
          }
        }
        console.log(`✓ Reassigned: ${item.title} → ${handles.join(', ')}`);
        await delay(500);
      } catch(e) { console.error(`Failed: ${item.title}`, e.message); }
    }
    console.log('[FIX-ALL] Done');
  })();
});

// ── AUTO-GODKÄNN KÖN ──────────────────────────────────────
app.post('/api/approve-all', async (req, res) => {
  const { min_score = 65 } = req.body || {};
  const eligible = store.queue.filter(p => p.status === 'pending' && p.score >= min_score);
  if (eligible.length === 0) return res.json({ ok: true, message: 'Inga att godkänna', approved: 0 });
  res.json({ ok: true, message: `Godkänner ${eligible.length} produkter...`, total: eligible.length });
  (async () => {
    const delay = ms => new Promise(r => setTimeout(r, ms));
    for (const item of eligible) {
      try {
        const shopifyProduct = await publishToShopify(item);
        item.status = 'approved';
        item.shopifyId = shopifyProduct.id;
        item.approvedAt = new Date().toISOString();
        store.products.push(item);
        store.queue = store.queue.filter(p => p.id !== item.id);
        store.stats.totalApproved++;
        await dbSave('products', item);
        await dbDelete('queue', item.id);
        console.log(`✓ Approved: ${item.title}`);
        await delay(2000);
      } catch(err) { console.error(`✗ Failed: ${item.title}`, err.message); }
    }
    console.log('[APPROVE-ALL] Done');
  })();
});


app.post('/api/research/keyword', async (req, res) => {
  const { keyword } = req.body;
  if (!keyword) return res.status(400).json({ error: 'keyword required' });
  res.json({ ok: true, message: `Researching: ${keyword}` });
  runProductResearch([keyword]);
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


// ── REDIGERA LIVE-PRODUKT ──────────────────────────────────
// Uppdaterar titel, pris, startbild och beskrivning direkt i Shopify.
// Body: { title?, sellPrice?, description?, primaryImage?, images? }
app.patch('/api/products/:id', async (req, res) => {
  const item = store.products.find(p => p.id === req.params.id || p.shopifyId == req.params.id);
  if (!item) return res.status(404).json({ error: 'Produkten hittades inte' });
  if (!item.shopifyId) return res.status(400).json({ error: 'Ingen Shopify-koppling — produkten är inte live' });

  const domain = process.env.SHOPIFY_DOMAIN;
  const token  = process.env.SHOP_TOKEN || process.env.SHOPIFY_TOKEN;
  if (!domain || !token) return res.status(500).json({ error: 'Shopify ej konfigurerat' });

  const { title, sellPrice, description, primaryImage, images } = req.body;
  const updates = {};

  if (title)       updates.title     = title;
  if (description) updates.body_html = `<p>${description.replace(/\n\n/g, '</p><p>')}</p>`;
  if (sellPrice) {
    updates.variants = [{ id: item.shopifyVariantId, price: String(sellPrice) }];
  }

  // Re-order images so primaryImage comes first
  if (primaryImage || images) {
    const allImages = images || item.images || [];
    const ordered   = primaryImage
      ? [primaryImage, ...allImages.filter(u => u !== primaryImage)]
      : allImages;
    updates.images = ordered
      .filter(u => u && u.startsWith('http'))
      .slice(0, 5)
      .map((url, i) => ({ src: url, position: i + 1 }));
  }

  if (Object.keys(updates).length === 0) {
    return res.status(400).json({ error: 'Inga fält att uppdatera skickades' });
  }

  try {
    const shopifyRes = await axios.put(
      `https://${domain}/admin/api/2024-01/products/${item.shopifyId}.json`,
      { product: updates },
      { headers: { 'X-Shopify-Access-Token': token, 'Content-Type': 'application/json' } }
    );
    const updated = shopifyRes.data.product;

    // Mirror changes locally
    if (title)      item.title     = title;
    if (sellPrice)  item.sellPrice = sellPrice;
    if (description) item.description = description;
    if (primaryImage) {
      item.images = [primaryImage, ...(item.images || []).filter(u => u !== primaryImage)];
    }
    if (images) item.images = images;

    // Persist to DB
    await dbSave('products', item);

    res.json({ ok: true, shopifyProduct: updated, local: item });
  } catch (err) {
    const errMsg = err.response?.data?.errors || err.message;
    res.status(500).json({ error: errMsg });
  }
});

// Hämta variant-ID för en live-produkt (behövs för prisändring)
app.get('/api/products/:id/variants', async (req, res) => {
  const item = store.products.find(p => p.id === req.params.id || p.shopifyId == req.params.id);
  if (!item?.shopifyId) return res.status(404).json({ error: 'Produkt eller Shopify-koppling saknas' });

  const domain = process.env.SHOPIFY_DOMAIN;
  const token  = process.env.SHOP_TOKEN || process.env.SHOPIFY_TOKEN;
  try {
    const r = await axios.get(
      `https://${domain}/admin/api/2024-01/products/${item.shopifyId}/variants.json`,
      { headers: { 'X-Shopify-Access-Token': token } }
    );
    res.json({ variants: r.data.variants });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
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


// ── UPPDATERA KOLLEKTIONSBILDER (Unsplash) ───────────────────
app.get('/api/update-collection-images', async (req, res) => {
  try {
    const { main } = require('./updateCollectionImages');
    res.json({ ok: true, message: 'Bildsuppdatering startad i bakgrunden' });
    main().then(r => console.log('✓ Kollektionsbilder uppdaterade'))
          .catch(e => console.error('Bilduppdatering misslyckades:', e.message));
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});


// ── CRON: Uppdatera kollektionsbilder 1 gång/månad ────────────
cron.schedule('0 3 1 * *', () => {
  console.log('Cron: Uppdaterar kollektionsbilder från Unsplash...');
  try {
    const { main } = require('./updateCollectionImages');
    main().catch(e => console.error('Månatlig bilduppdatering misslyckades:', e.message));
  } catch(e) {
    console.error('updateCollectionImages inte laddad:', e.message);
  }
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
  if (startScoreCron && db) {
    startScoreCron(db);
    console.log('✓ Score cron started');
  }
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
