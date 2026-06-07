// Mercury AI — Service Worker
// Handles offline caching and push notifications

const CACHE_NAME = 'mercury-ai-v1';
const ASSETS = [
  '/index.html',
  '/manifest.json',
];

// ── INSTALL: Cache core assets ─────────────
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => {
      console.log('[SW] Caching core assets');
      return cache.addAll(ASSETS);
    })
  );
  self.skipWaiting();
});

// ── ACTIVATE: Clean old caches ─────────────
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys.filter(key => key !== CACHE_NAME)
            .map(key => caches.delete(key))
      )
    )
  );
  self.clients.claim();
});

// ── FETCH: Network first, cache fallback ───
self.addEventListener('fetch', event => {
  // Don't intercept API calls or external resources
  if (event.request.url.includes('/api/') ||
      event.request.url.includes('api.openai.com') ||
      event.request.url.includes('myshopify.com') ||
      !event.request.url.startsWith(self.location.origin)) {
    return;
  }

  event.respondWith(
    fetch(event.request)
      .then(response => {
        // Cache successful GET responses
        if (event.request.method === 'GET' && response.status === 200) {
          const clone = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
        }
        return response;
      })
      .catch(() => {
        // Offline fallback
        return caches.match(event.request).then(cached => {
          if (cached) return cached;
          // Return offline page for navigation requests
          if (event.request.mode === 'navigate') {
            return caches.match('/index.html');
          }
        });
      })
  );
});

// ── PUSH NOTIFICATIONS ─────────────────────
self.addEventListener('push', event => {
  let data = { title: 'Mercury AI', body: 'New update available' };
  try { data = event.data.json(); } catch(e) {}

  event.waitUntil(
    self.registration.showNotification(data.title || 'Mercury AI', {
      body: data.body || '',
      icon: '/icon-192.png',
      badge: '/icon-72.png',
      tag: data.tag || 'mercury-notification',
      data: { url: data.url || '/' },
      actions: [
        { action: 'open', title: 'Open Mercury' },
        { action: 'dismiss', title: 'Dismiss' }
      ]
    })
  );
});

// ── NOTIFICATION CLICK ─────────────────────
self.addEventListener('notificationclick', event => {
  event.notification.close();

  if (event.action === 'dismiss') return;

  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true })
      .then(clientList => {
        // Focus existing window if open
        for (const client of clientList) {
          if (client.url.includes(self.location.origin) && 'focus' in client) {
            return client.focus();
          }
        }
        // Open new window
        return clients.openWindow(event.notification.data?.url || '/');
      })
  );
});

// ── BACKGROUND SYNC (check for new products) ─
self.addEventListener('sync', event => {
  if (event.tag === 'check-queue') {
    event.waitUntil(checkForNewProducts());
  }
});

async function checkForNewProducts() {
  try {
    const cache = await caches.open(CACHE_NAME);
    const configRes = await cache.match('mercury-config');
    if (!configRes) return;
    const config = await configRes.json();
    if (!config.backendUrl) return;

    const res = await fetch(`${config.backendUrl}/api/status`);
    const data = await res.json();

    if (data.queueCount > 0) {
      await self.registration.showNotification('Mercury AI', {
        body: `${data.queueCount} product${data.queueCount > 1 ? 's' : ''} ready for your review`,
        icon: '/icon-192.png',
        tag: 'queue-update',
        data: { url: '/index.html' }
      });
    }
  } catch(e) {
    console.log('[SW] Background sync failed:', e);
  }
}
