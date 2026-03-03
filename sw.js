const CACHE_NAME = 'mora-enhanced-v7';

const PRECACHE_URLS = [
  './manifest.json'
];

function isNavigationRequest(request) {
  return request.mode === 'navigate' || (request.destination === 'document');
}

function isStaticAsset(request) {
  return (
    request.destination === 'style' ||
    request.destination === 'script' ||
    request.destination === 'image' ||
    request.destination === 'font'
  );
}

self.addEventListener('install', (event) => {
  event.waitUntil(caches.open(CACHE_NAME).then((cache) => cache.addAll(PRECACHE_URLS)));
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.map((key) => (key !== CACHE_NAME ? caches.delete(key) : null)))
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  const { request } = event;
  if (request.method !== 'GET') return;

  // Never cache index.html. Telegram WebView can keep showing an old build otherwise.
  try {
    const u = new URL(request.url);
    if (u.origin === self.location.origin && /\/index\.html(\?|#|$)/.test(u.pathname + (u.search||'') + (u.hash||''))) {
      event.respondWith(fetch(request, { cache: 'no-store' }));
      return;
    }
  } catch (e) {}

  if (isNavigationRequest(request)) {
    event.respondWith(
      fetch(request, { cache: 'no-store' })
        .catch(() => fetch('./index.html', { cache: 'no-store' }))
    );
    return;
  }

  if (isStaticAsset(request)) {
    event.respondWith(
      caches.match(request).then((cached) => {
        if (cached) return cached;
        return fetch(request)
          .then((response) => {
            const url = new URL(request.url);
            if (url.origin === self.location.origin) {
              const copy = response.clone();
              caches.open(CACHE_NAME).then((cache) => cache.put(request, copy));
            }
            return response;
          });
      })
    );
    return;
  }

  event.respondWith(
    fetch(request).catch(() => caches.match(request))
  );
});
