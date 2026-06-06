const CACHE_NAME = "licitabayes-dncp-v0-1-10";
const APP_SHELL = [
  "./",
  "index.html",
  "assets/styles.css",
  "assets/config.js",
  "assets/app.js",
  "manifest.json",
  "data/model_summary.json",
  "data/price_alerts.json",
  "data/concentration_alerts.json",
  "data/series.json"
];

self.addEventListener("install", (event) => {
  event.waitUntil(caches.open(CACHE_NAME).then((cache) => cache.addAll(APP_SHELL)));
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) => Promise.all(keys.filter((key) => key !== CACHE_NAME).map((key) => caches.delete(key))))
  );
});

self.addEventListener("fetch", (event) => {
  if (event.request.method !== "GET") return;
  event.respondWith(
    fetch(event.request)
      .then((response) => {
        const copy = response.clone();
        caches.open(CACHE_NAME).then((cache) => cache.put(event.request, copy));
        return response;
      })
      .catch(() => caches.match(event.request).then((cached) => cached || caches.match("index.html")))
  );
});
