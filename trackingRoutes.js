/**
 * MERCURY — TRACKING ROUTES
 * 
 * Mount in server.js:
 *   const trackingRoutes = require('./trackingRoutes');
 *   app.use('/track', trackingRoutes(db));
 * 
 * Endpoints:
 *   POST /track/click          — called from Vintera frontend (product card hover/click)
 *   POST /track/conversion     — called from Shopify order webhook
 *   POST /track/product-created — called when Mercury publishes a new product to Shopify
 */

const express = require('express');
const crypto  = require('crypto');

module.exports = function trackingRoutes(db) {
  const router = express.Router();

  // ── CLICK TRACKING ─────────────────────────────────────────────────────────
  // Body: { product_id: "1234567890", session_id: "abc123" }
  // Called by vintera.js when user clicks a product card.
  // Rate-limited: max 1 click per session per product per 30 min (in-memory map).
  
  const recentClicks = new Map(); // "productId:sessionId" → timestamp

  router.post('/click', async (req, res) => {
    const { product_id, session_id } = req.body;

    if (!product_id) {
      return res.status(400).json({ error: 'product_id required' });
    }

    // Deduplicate: same session can't register more than 1 click per 30 min
    const dedupeKey  = `${product_id}:${session_id || 'anon'}`;
    const lastClick  = recentClicks.get(dedupeKey);
    const thirtyMin  = 30 * 60 * 1000;

    if (lastClick && Date.now() - lastClick < thirtyMin) {
      return res.json({ ok: true, deduplicated: true });
    }

    recentClicks.set(dedupeKey, Date.now());

    // Prune map every 1000 entries to avoid memory leak
    if (recentClicks.size > 1000) {
      const cutoff = Date.now() - thirtyMin;
      for (const [k, v] of recentClicks) {
        if (v < cutoff) recentClicks.delete(k);
      }
    }

    try {
      // Upsert stats row (ensure product exists in tracking table)
      await db.query(`
        INSERT INTO product_stats (shopify_product_id, clicks)
        VALUES (?, 1)
        ON CONFLICT (shopify_product_id) DO UPDATE SET
          clicks     = clicks + 1,
          updated_at = CURRENT_TIMESTAMP
      `, [product_id]);

      // Log event for time-decay analysis
      await db.query(`
        INSERT INTO click_events (shopify_product_id, session_id, referrer)
        VALUES (?, ?, ?)
      `, [product_id, session_id || null, req.get('Referer') || null]);

      res.json({ ok: true });
    } catch (e) {
      console.error('[track/click]', e.message);
      res.status(500).json({ error: 'DB error' });
    }
  });

  // ── CONVERSION TRACKING (Shopify order webhook) ────────────────────────────
  // Shopify sends this when an order is created.
  // Set up in Shopify Admin → Settings → Notifications → Webhooks
  // Topic: orders/create  URL: https://mercury-production-xxx.up.railway.app/track/conversion
  
  router.post('/conversion', async (req, res) => {
    // Verify Shopify webhook signature
    const hmacHeader = req.get('X-Shopify-Hmac-Sha256');
    const secret     = process.env.SHOPIFY_WEBHOOK_SECRET;

    if (secret && hmacHeader) {
      const digest = crypto
        .createHmac('sha256', secret)
        .update(req.rawBody || JSON.stringify(req.body))
        .digest('base64');

      if (digest !== hmacHeader) {
        console.warn('[track/conversion] Invalid webhook signature');
        return res.status(401).json({ error: 'Invalid signature' });
      }
    }

    const order = req.body;
    if (!order || !order.line_items) {
      return res.status(400).json({ error: 'Invalid order payload' });
    }

    try {
      for (const item of order.line_items) {
        const productId = String(item.product_id);
        const quantity  = item.quantity || 1;
        const revenue   = parseFloat(item.price) * quantity;

        // Update stats
        await db.query(`
          INSERT INTO product_stats (shopify_product_id, conversions)
          VALUES ($1, $2)
          ON CONFLICT (shopify_product_id) DO UPDATE SET
            conversions = product_stats.conversions + $3,
            updated_at = CURRENT_TIMESTAMP
        `, [productId, quantity, quantity]);

        // Log conversion event
        await db.query(`
          INSERT INTO conversion_events (shopify_product_id, order_id, quantity, revenue_sek)
          VALUES ($1, $2, $3, $4)
        `, [productId, String(order.id), quantity, revenue]);
      }

      res.json({ ok: true, items_tracked: order.line_items.length });
    } catch (e) {
      console.error('[track/conversion]', e.message);
      res.status(500).json({ error: 'DB error' });
    }
  });

  // ── PRODUCT CREATED (called by Mercury publish pipeline) ───────────────────
  // Body: { shopify_product_id: "123", product_title: "Akupressursmatta" }
  // Called automatically by Mercury's publishProduct() function after Shopify create.
  // Registers the product in scoring system with published_at = now.

  router.post('/product-created', async (req, res) => {
    const { shopify_product_id, product_title } = req.body;

    if (!shopify_product_id) {
      return res.status(400).json({ error: 'shopify_product_id required' });
    }

    try {
      await db.query(`
        INSERT INTO product_stats (shopify_product_id, product_title, published_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (shopify_product_id) DO NOTHING
      `, [String(shopify_product_id), product_title || ""]);

      res.json({ ok: true });
    } catch (e) {
      console.error('[track/product-created]', e.message);
      res.status(500).json({ error: 'DB error' });
    }
  });

  // ── STATS ENDPOINT (Mercury dashboard) ─────────────────────────────────────
  // GET /track/stats — returns top 20 products by score, for Mercury dashboard.

  router.get('/stats', async (req, res) => {
    try {
      const result = await db.query(`
        SELECT
          shopify_product_id,
          product_title,
          clicks,
          conversions,
          last_score,
          published_at,
          updated_at
        FROM product_stats
        ORDER BY last_score DESC
        LIMIT 50
      `);
      res.json({ ok: true, products: rows });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  });

  return router;
};
