/**
 * MERCURY — DB MIGRATION
 * Run once on deploy: node migrate.js
 * Adds the product_stats table to the existing SQLite DB.
 */

const Database = require('better-sqlite3');
const path     = require('path');

const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'mercury.db');
const db      = new Database(DB_PATH);

db.exec(`
  CREATE TABLE IF NOT EXISTS product_stats (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    shopify_product_id  TEXT    NOT NULL UNIQUE,
    product_title       TEXT,
    published_at        TEXT    NOT NULL DEFAULT (datetime('now')),
    clicks              INTEGER NOT NULL DEFAULT 0,
    conversions         INTEGER NOT NULL DEFAULT 0,
    last_score          INTEGER NOT NULL DEFAULT 0,
    updated_at          TEXT    NOT NULL DEFAULT (datetime('now'))
  );

  CREATE INDEX IF NOT EXISTS idx_product_stats_shopify_id
    ON product_stats (shopify_product_id);

  CREATE INDEX IF NOT EXISTS idx_product_stats_score
    ON product_stats (last_score DESC);
`);

// Click events log (for time-decay calculations and debugging)
db.exec(`
  CREATE TABLE IF NOT EXISTS click_events (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    shopify_product_id TEXT NOT NULL,
    session_id         TEXT,
    referrer           TEXT,
    created_at         TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE INDEX IF NOT EXISTS idx_click_events_product
    ON click_events (shopify_product_id, created_at);
`);

// Conversion events log
db.exec(`
  CREATE TABLE IF NOT EXISTS conversion_events (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    shopify_product_id TEXT NOT NULL,
    order_id           TEXT,
    quantity           INTEGER DEFAULT 1,
    revenue_sek        REAL    DEFAULT 0,
    created_at         TEXT    NOT NULL DEFAULT (datetime('now'))
  );

  CREATE INDEX IF NOT EXISTS idx_conversion_events_product
    ON conversion_events (shopify_product_id, created_at);
`);

console.log('✅ product_stats, click_events, conversion_events tables ready.');
db.close();
