require('dotenv').config();
const express = require('express');
const cors = require('cors');
const cron = require('node-cron');
const axios = require('axios');
const OpenAI = require('openai');

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

// ── CJ DROPSHIPPING API ────────────────────────────────────
async function getCJToken() {
  try {
    const res = await axios.post('https://developers.cjdropshipping.com/api2.0/v1/authentication/getAccessToken', {
      email: process.env.CJ_EMAIL,
      password: process.env.CJ_PASSWORD
    });
    return res.data?.data?.accessToken || null;
  } catch(e) {
    console.error('CJ auth failed:', e.message);
    return null;
  }
}

async function searchCJProducts(token, keyword, limit = 20) {
  try {
    const res = await axios.get('https://developers.cjdropshipping.com/api2.0/v1/product/list', {
      headers: { 'CJ-Access-Token': token },
      params: {
        productNameEn: keyword,
        pageNum: 1,
        pageSize: limit,
        orderBy: 'ORDER_COUNT',
        orderType: 'DESC'
      }
    });
    return res.data?.data?.list || [];
  } catch(e) {
    console.error('CJ search failed:', e.message);
    return [];
  }
}

async function getCJProductDetail(token, pid) {
  try {
    const res = await axios.get(`https://developers.cjdropshipping.com/api2.0/v1/product/query`, {
      headers: { 'CJ-Access-Token': token },
      params: { pid }
    });
    return res.data?.data || null;
  } catch(e) {
    return null;
  }
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
  const prompt = `You are a premium copywriter for Vintera, a curated modern lifestyle store. Transform this supplier product into polished, conversion-ready copy.

Product: ${rawProduct.nameEn || rawProduct.name}
Category: ${rawProduct.categoryName || 'General'}
Supplier description: ${rawProduct.description || 'Not provided'}
Cost price: ${rawProduct.sellPrice || '?'} USD

Write EXACTLY in this format:

TITLE: [Clean brand-style title, max 8 words, no supplier language]

META: [SEO meta description, max 155 chars, benefit-first]

DESCRIPTION: [2 short paragraphs, premium lifestyle tone, no hyperbole]

BENEFITS:
• [Benefit with bold lead word — specific and useful]
• [Benefit with bold lead word — specific and useful]
• [Benefit with bold lead word — specific and useful]

FAQ:
Q: [Most asked question about this product]
A: [Confident, helpful answer]

AD_HOOK: [TikTok/Meta hook, max 10 words, curiosity or problem-solution]

TAGS: [5 relevant tags, comma separated, lowercase]

Keep everything clean, modern, trustworthy. No "dropshipping", no supplier names, no fake claims.`;

  const res = await openai.chat.completions.create({
    model: 'gpt-4o',
    max_tokens: 800,
    messages: [
      { role: 'system', content: 'You are a premium e-commerce copywriter. Return only the formatted content requested, nothing else.' },
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
    // Get CJ token
    const token = await getCJToken();
    if (!token) {
      console.error('No CJ token — check CJ_EMAIL and CJ_PASSWORD in .env');
      store.syncStatus = 'error';
      return;
    }

    // Pick 3 random keywords this cycle
    const shuffle = arr => [...arr].sort(() => Math.random() - 0.5);
    const keywords = shuffle(TREND_KEYWORDS).slice(0, 3);
    console.log('Researching keywords:', keywords);

    let candidates = [];

    for (const keyword of keywords) {
      const products = await searchCJProducts(token, keyword, 10);
      store.stats.totalScanned += products.length;

      products.forEach((p, i) => {
        const score = scoreProduct(p, i);
        if (score >= 55) {
          candidates.push({ ...p, score, keyword });
        }
      });
    }

    // Sort by score, take top 5
    candidates.sort((a, b) => b.score - a.score);
    const top = candidates.slice(0, 5);

    console.log(`Found ${candidates.length} candidates, processing top ${top.length}`);

    for (const product of top) {
      // Skip if already in queue or approved
      const exists = [...store.queue, ...store.products].find(
        p => p.cjPid === product.pid
      );
      if (exists) continue;

      try {
        // Generate AI content
        const rawContent = await generateProductContent(product);
        const content = parseGeneratedContent(rawContent);

        // Pick best 5 images
        const images = (product.productImageSet || [])
          .filter(img => img && img.startsWith('http'))
          .slice(0, 5);

        const queueItem = {
          id: `q_${Date.now()}_${Math.random().toString(36).slice(2,7)}`,
          cjPid: product.pid,
          score: product.score,
          keyword: product.keyword,
          costPrice: parseFloat(product.sellPrice) || 0,
          sellPrice: Math.round(parseFloat(product.sellPrice) * 5 * 9.5), // USD to SEK approx
          margin: Math.round(((parseFloat(product.sellPrice)*5 - parseFloat(product.sellPrice)) / (parseFloat(product.sellPrice)*5)) * 100),
          category: product.categoryName || 'General',
          stock: parseInt(product.inventory) || 99,
          shippingDays: parseInt(product.shippingTime) || 14,
          images,
          // AI-generated content
          title: content.title || product.nameEn,
          meta: content.meta,
          description: content.description,
          descriptionHtml: content.descriptionHtml,
          benefits: content.benefits,
          adHook: content.adHook,
          tags: content.tags,
          // Raw
          rawTitle: product.nameEn,
          addedAt: new Date().toISOString(),
          status: 'pending'
        };

        store.queue.push(queueItem);
        store.stats.totalQueued++;
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
app.post('/api/reject/:id', (req, res) => {
  const item = store.queue.find(p => p.id === req.params.id);
  if (!item) return res.status(404).json({ error: 'Not found' });
  item.status = 'rejected';
  store.removed.push(item);
  store.queue = store.queue.filter(p => p.id !== req.params.id);
  store.stats.totalRejected++;
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
cron.schedule('0 */6 * * *', () => {
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
app.listen(PORT, () => {
  console.log(`Mercury Backend running on port ${PORT}`);
  console.log('Shopify:', process.env.SHOPIFY_DOMAIN || 'NOT SET');
  console.log('OpenAI:', process.env.OPENAI_API_KEY ? 'SET' : 'NOT SET');
  console.log('CJ:', process.env.CJ_EMAIL || 'NOT SET');
  console.log('Make.com:', process.env.MAKE_WEBHOOK_URL ? 'SET' : 'NOT SET');
  console.log('Email:', process.env.RESEND_API_KEY ? 'SET' : 'NOT SET');
  console.log('Orbit:', process.env.ORBIT_API_URL ? 'SET' : 'NOT SET');
  // Sync performance on startup
  setTimeout(syncShopifyPerformance, 10000);
  // Run first research on startup
  setTimeout(runProductResearch, 5000);
});
