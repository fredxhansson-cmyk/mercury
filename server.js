require('dotenv').config();
const express = require('express');
const cors = require('cors');
const cron = require('node-cron');
const axios = require('axios');
const OpenAI = require('openai');
const { Pool } = require('pg');

// ── DATABASE ───────────────────────────────────────────────
let db = null;
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

// ── ALIEXPRESS DATAHUB API (via RapidAPI) ─────────────────
async function searchAliExpressProducts(keyword, limit = 20) {
  const rapidApiKey = process.env.RAPIDAPI_KEY;
  if (!rapidApiKey) {
    console.error('No RAPIDAPI_KEY set');
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
    console.error('AliExpress search failed:', e.message);
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
  try {
    const res = await axios.post('https://developers.cjdropshipping.com/api2.0/v1/authentication/getAccessToken', {
      email: process.env.CJ_EMAIL,
      password: process.env.CJ_PASSWORD
    });
    if (res.data?.data?.accessToken) {
      console.log('✓ CJ token acquired');
      return res.data.data.accessToken;
    }
    console.error('CJ auth failed:', res.data?.message);
    return null;
  } catch(e) {
    console.error('CJ auth error:', e.message);
    return null;
  }
}

async function searchCJProducts(token, keyword, limit = 20) {
  try {
    const res = await axios.get('https://developers.cjdropshipping.com/api2.0/v1/product/list', {
      headers: { 'CJ-Access-Token': token },
      params: { productNameEn: keyword, pageNum: 1, pageSize: limit, orderBy: 'ORDER_COUNT', orderType: 'DESC' }
    });
    return res.data?.data?.list || [];
  } catch(e) {
    console.error('CJ search failed:', e.message);
    return [];
  }
}

function scoreCJProduct(product, index) {
  let score = 0;
  const price = parseFloat(product.sellPrice) || 10;
  if (price >= 2 && price <= 80) score += 20;
  else score += 8;
  const orders = parseInt(product.orderCount) || 0;
  score += Math.min(25, orders / 100);
  const imgs = product.productImageSet?.length || 0;
  score += Math.min(15, imgs * 3);
  const ship = parseInt(product.shippingTime) || 20;
  score += Math.max(0, 15 - ship);
  if (product.variants?.length > 1) score += 10;
  score += Math.max(0, 15 - index);
  return Math.min(100, Math.round(score));
}

// ── SHOPIFY API ────────────────────────────────────────────
async function publishToShopify(product) {
  const domain = process.env.SHOPIFY_DOMAIN;
  const token  = process.env.SHOPIFY_TOKEN;
  if(!domain || !token) throw new Error('Shopify not configured');

  const payload = {
    product: {
      title: product.title,
      body_html: product.descriptionHtml,
      vendor: 'Vintera',
      product_type: product.category,
      tags: product.tags?.join(','),
      status: 'active',
      variants: [{
        price: product.sellPrice.toString(),
        sku: `VIN-${product.cjPid}`,
        inventory_management: 'shopify',
        inventory_quantity: product.stock || 50,
        weight: product.weight || 0.3,
        weight_unit: 'kg'
      }],
      images: product.images?.slice(0, 5).map(url => ({ src: url }))
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
  const prompt = `Du är en premium copywriter för Vintera, en modern kurerad livsstilsbutik som säljer i Sverige. Förvandla denna produkt till polerad, konverteringsfokuserad copy på SVENSKA.

Produkt: ${rawProduct.nameEn || rawProduct.name}
Kategori: ${rawProduct.categoryName || 'Allmänt'}
Beskrivning: ${rawProduct.description || 'Inte angiven'}
Kostpris: ${rawProduct.sellPrice || '?'} USD

Skriv EXAKT i detta format:

TITLE: [Rent varumärkestitel, max 8 ord, inget leverantörsspråk, på svenska]

META: [SEO-metabeskrivning, max 155 tecken, fördel-först, på svenska]

DESCRIPTION: [2 korta stycken, premium livsstilston, ingen överdrift, på svenska]

BENEFITS:
• [Fördel med fetstilt ledord — specifik och användbar, på svenska]
• [Fördel med fetstilt ledord — specifik och användbar, på svenska]
• [Fördel med fetstilt ledord — specifik och användbar, på svenska]

FAQ:
Q: [Vanligaste frågan om produkten, på svenska]
A: [Säkert, hjälpsamt svar, på svenska]

AD_HOOK: [TikTok/Meta-hook, max 10 ord, nyfikenhet eller problem-lösning, på svenska]

TAGS: [5 relevanta taggar, kommaseparerade, gemener, på svenska]

Håll allt rent, modernt och trovärdigt. Inget "dropshipping", inga leverantörsnamn, inga falska påståenden.`;

  const res = await openai.chat.completions.create({
    model: 'gpt-4o',
    max_tokens: 800,
    messages: [
      { role: 'system', content: 'Du är en premium e-handelscopywriter för svenska marknaden. Returnera endast det formaterade innehållet som efterfrågas, inget annat. All text ska vara på svenska.' },
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
  'posture corrector', 'led desk lamp', 'portable blender',
  'acupressure mat', 'cable organizer', 'pet hair remover',
  'resistance bands', 'phone mount', 'massage gun',
  'sleep mask', 'back stretcher', 'knee brace',
  'air purifier mini', 'water bottle insulated', 'face massager',
  'eye mask heated', 'back massager', 'laptop stand portable',
  'ring light mini', 'fitness tracker band'
];

// ── MAIN RESEARCH PIPELINE ─────────────────────────────────
async function runProductResearch() {
  if (store.syncStatus === 'running') return;
  store.syncStatus = 'running';
  store.lastSync = new Date().toISOString();
  console.log(`[${new Date().toISOString()}] Starting product research...`);

  try {
    const shuffle = arr => [...arr].sort(() => Math.random() - 0.5);
    const keywords = shuffle(TREND_KEYWORDS).slice(0, 3);
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
          const products = await searchCJProducts(cjToken, keyword, 10);
          store.stats.totalScanned += products.length;
          products.forEach((p, i) => {
            const score = scoreCJProduct(p, i);
            if (score >= 40) {
              candidates.push({
                ...p,
                score,
                keyword,
                source: 'cj',
                title: p.nameEn || p.name,
                itemId: p.pid,
                image: p.productImageSet?.[0] || '',
                salePrice: p.sellPrice,
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
    const top = candidates.slice(0, 5);

    console.log(`Found ${candidates.length} candidates, processing top ${top.length}`);

    for (const product of top) {
      // Skip if already in queue or approved
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
        // Get images from product data (avoid extra API call)
        const images = [];
        const imgBase = product.image || product.imageUrl || '';
        if (imgBase) {
          // Fix protocol-relative URLs
          const fixUrl = u => u.startsWith('//') ? 'https:' + u : u;
          images.push(fixUrl(imgBase));
        }
        // Try to get more images from detail (non-blocking)
        try {
          const detail = await getAliExpressProductDetail(product.itemId||product.productId);
          if (detail?.imageUrl) images.push(detail.imageUrl.startsWith('//') ? 'https:'+detail.imageUrl : detail.imageUrl);
        } catch(e) {}

        const costUsd = Math.max(2, parseFloat(product.sku?.def?.promotionPrice || product.sku?.def?.price || product.price?.minPrice || product.salePrice || 5));
        const rawSek = Math.round(costUsd * 5 * 9.5); // 5x markup, USD to SEK
        const sellSek = snapPrice(rawSek);

        // Generate AI content
        const productName = product.title || product.name || product.subject || 'Trending Product';
        const rawContent = await generateProductContent({
          nameEn: productName,
          description: productName,
          sellPrice: costUsd,
          categoryName: product.productType || product.category || 'General'
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
          tags: content.tags,
          // Raw
          rawTitle: product.title || product.name || product.subject || 'Product',
          aliUrl: `https://www.aliexpress.com/item/${product.itemId||product.productId}.html`,
          addedAt: new Date().toISOString(),
          status: 'pending'
        };

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
  const token  = process.env.SHOPIFY_TOKEN;
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
    item.approvedAt = new Date().toISOString();

    store.products.push(item);
    store.queue = store.queue.filter(p => p.id !== req.params.id);
    store.stats.totalApproved++;
    await dbSave('products', item);
    await dbDelete('queue', item.id);

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
  console.log(`Mercury Backend running on port ${PORT}`);
  console.log('Shopify:', process.env.SHOPIFY_DOMAIN || 'NOT SET');
  console.log('OpenAI:', process.env.OPENAI_API_KEY ? 'SET' : 'NOT SET');
  console.log('RapidAPI:', process.env.RAPIDAPI_KEY ? 'SET' : 'NOT SET');
  console.log('CJ:', process.env.CJ_EMAIL ? process.env.CJ_EMAIL : 'NOT SET');
  console.log('Make.com:', process.env.MAKE_WEBHOOK_URL ? 'SET' : 'NOT SET');
  console.log('Email:', process.env.RESEND_API_KEY ? 'SET' : 'NOT SET');
  console.log('Orbit:', process.env.ORBIT_API_URL ? 'SET' : 'NOT SET');
  // Sync performance on startup
  setTimeout(syncShopifyPerformance, 10000);
  // Run first research on startup
  setTimeout(runProductResearch, 5000);
});
