// 셔틀Go service worker (v0.9.6, P1-B layer 2).
//
// Fixed logic: this file does not change from one deploy to the next, so a new
// deploy never leaves a "waiting" worker behind. What to cache comes from
// /asset-manifest.json, which the site build writes.
//
// - Navigation to "/" is answered from the cached page shell at once; the
//   shell is refreshed in the background for the next visit.
// - A new shell is stored only after every hashed asset it names is cached,
//   and only if it carries the same build id as the manifest. (The id is
//   compared instead of a hash of the HTML because the CDN may add its own
//   tags to the page.)
// - /assets/* and /fonts/* are cache-first (their URLs are content-versioned).
// - Everything else (API, map SDK, analytics, /page/*, auth pages, POST) is not
//   touched.
// - A cached shell older than 7 days is tried against the network first.
//
// The page registers this worker only for devices inside the
// "service_worker_percent" switch and unregisters it otherwise.
'use strict';

var SHELL_CACHE = 'sg-shell-v1';
var ASSET_CACHE = 'sg-assets-v1';
var META_CACHE = 'sg-meta-v1';
var SHELL_KEY = '/';
var META_KEY = '/__sg_meta__';
var MANIFEST_URL = '/asset-manifest.json';
var SHELL_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;
var NETWORK_FIRST_TIMEOUT_MS = 1500;
var refreshing = null;

function readMeta() {
  return caches.open(META_CACHE).then(function(cache) { return cache.match(META_KEY); })
    .then(function(res) { return res ? res.json() : {}; })
    .catch(function() { return {}; });
}

function writeMeta(meta) {
  return caches.open(META_CACHE).then(function(cache) {
    return cache.put(META_KEY, new Response(JSON.stringify(meta), { headers: { 'Content-Type': 'application/json' } }));
  });
}

// The site build stamps the page with: window.__SG_BUILD="<revision>";
function buildIdOf(html) {
  var match = /window\.__SG_BUILD="([^"]{1,64})"/.exec(html);
  return match ? match[1] : '';
}

function isAssetPath(pathname) {
  return pathname.indexOf('/assets/') === 0 || pathname.indexOf('/fonts/') === 0;
}

// Fetch the shell and its manifest; store them only as a consistent pair.
function refreshShell() {
  if (refreshing) return refreshing;
  refreshing = Promise.all([
    fetch(SHELL_KEY, { cache: 'no-cache', credentials: 'same-origin' }),
    fetch(MANIFEST_URL, { cache: 'no-cache', credentials: 'same-origin' })
  ]).then(function(results) {
    var shellRes = results[0], manifestRes = results[1];
    if (!shellRes.ok || !manifestRes.ok) throw new Error('shell or manifest unavailable');
    return Promise.all([shellRes.text(), manifestRes.json(), readMeta(), cachedShell()]).then(function(parts) {
      var html = parts[0], manifest = parts[1], meta = parts[2], haveShell = !!parts[3];
      var assets = Array.isArray(manifest.assets) ? manifest.assets.filter(function(url) {
        return typeof url === 'string' && url.indexOf('/assets/') === 0;
      }) : [];
      return Promise.resolve(buildIdOf(html)).then(function(build) {
        // No build id (unbuilt page), or a deploy landed between the two requests:
        // store nothing and try again on the next visit.
        if (!build || !manifest.build || manifest.build !== build) throw new Error('shell and manifest differ');
        if (meta.build === build && haveShell) {
          meta.checkedAt = Date.now();
          return writeMeta(meta).then(function() { return 'unchanged'; });
        }
        return caches.open(ASSET_CACHE).then(function(assetCache) {
          // 1) every asset the new shell names, 2) then the shell itself.
          return Promise.all(assets.map(function(url) {
            return assetCache.match(url).then(function(hit) {
              if (hit) return null;
              return fetch(url, { credentials: 'same-origin' }).then(function(res) {
                if (!res.ok) throw new Error('asset unavailable: ' + url);
                return assetCache.put(url, res);
              });
            });
          })).then(function() {
            return caches.open(SHELL_CACHE).then(function(shellCache) {
              return shellCache.put(SHELL_KEY, new Response(html, {
                status: 200, headers: { 'Content-Type': 'text/html; charset=utf-8' }
              }));
            });
          }).then(function() {
            // Keep this build's assets and the previous build's (a tab may still show it).
            var keep = {};
            assets.concat(Array.isArray(meta.assets) ? meta.assets : []).forEach(function(url) { keep[url] = true; });
            return assetCache.keys().then(function(requests) {
              return Promise.all(requests.map(function(request) {
                var path = new URL(request.url).pathname;
                if (path.indexOf('/assets/') !== 0 || keep[path]) return null;
                return assetCache.delete(request);
              }));
            });
          }).then(function() {
            return writeMeta({ build: build, build_time: manifest.build_time || 0,
                               assets: assets, savedAt: Date.now(), checkedAt: Date.now() });
          }).then(function() { return 'updated'; });
        });
      });
    });
  }).then(function(result) { refreshing = null; return result; },
          function(error) { refreshing = null; throw error; });
  return refreshing;
}

function cachedShell() {
  return caches.open(SHELL_CACHE).then(function(cache) { return cache.match(SHELL_KEY); });
}

function networkShell(request) {
  return fetch(request).then(function(res) {
    refreshShell().catch(function() {});
    return res;
  });
}

function serveShell(event) {
  return Promise.all([cachedShell(), readMeta()]).then(function(parts) {
    var cached = parts[0], meta = parts[1];
    if (!cached) return networkShell(event.request);
    var age = Date.now() - Number(meta.checkedAt || meta.savedAt || 0);
    if (age >= 0 && age < SHELL_MAX_AGE_MS) {
      event.waitUntil(refreshShell().catch(function() {}));
      return cached;
    }
    // Too old: prefer the network, but do not keep the user waiting on a bad one.
    return new Promise(function(resolve) {
      var done = false;
      var timer = setTimeout(function() { if (!done) { done = true; resolve(cached); } }, NETWORK_FIRST_TIMEOUT_MS);
      networkShell(event.request).then(function(res) {
        if (done) return;
        done = true; clearTimeout(timer); resolve(res.ok ? res : cached);
      }, function() {
        if (done) return;
        done = true; clearTimeout(timer); resolve(cached);
      });
    });
  });
}

function serveAsset(request) {
  return caches.open(ASSET_CACHE).then(function(cache) {
    return cache.match(request).then(function(hit) {
      if (hit) return hit;
      return fetch(request).then(function(res) {
        if (res.ok && res.type === 'basic') cache.put(request, res.clone()).catch(function() {});
        return res;
      });
    });
  });
}

self.addEventListener('install', function(event) {
  self.skipWaiting();
  // Cache the shell now so the very next visit is already served from here.
  event.waitUntil(refreshShell().catch(function() {}));
});

self.addEventListener('activate', function(event) {
  event.waitUntil(self.clients.claim());
});

self.addEventListener('fetch', function(event) {
  var request = event.request;
  if (request.method !== 'GET') return;
  var url;
  try { url = new URL(request.url); } catch (e) { return; }
  if (url.origin !== self.location.origin) return;
  if (request.mode === 'navigate') {
    if (url.pathname === '/' || url.pathname === '/index.html') event.respondWith(serveShell(event));
    return;
  }
  if (isAssetPath(url.pathname)) event.respondWith(serveAsset(request));
});

// The page asks for an immediate refresh when the server says its build is too old.
self.addEventListener('message', function(event) {
  var data = event.data || {};
  if (data.type !== 'sg-refresh-shell') return;
  var port = event.ports && event.ports[0];
  var work = refreshShell().then(function(result) { return { ok: true, result: result }; },
                                 function(error) { return { ok: false, error: String(error && error.message || error) }; })
    .then(function(reply) { if (port) port.postMessage(reply); });
  if (event.waitUntil) event.waitUntil(work);
});
