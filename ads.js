(function () {
  'use strict';
  function element(tag, className, text) {
    var node = document.createElement(tag);
    if (className) node.className = className;
    if (text !== undefined) node.textContent = text;
    return node;
  }
  function nonce() {
    if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
    var bytes = new Uint8Array(16);
    crypto.getRandomValues(bytes);
    return Array.from(bytes).map(function (x) { return x.toString(16).padStart(2, '0'); }).join('');
  }
  function capacity(width) {
    if (width >= 1600) return 6;
    if (width >= 1440) return 5;
    if (width >= 1280) return 4;
    if (width >= 1024) return 3;
    if (width > 768) return 2;
    return 1;
  }
  function start(options) {
    var bottom = document.getElementById('adsBottom');
    var right = document.getElementById('adsRight');
    if (!bottom || !right) return null;
    var authReady = false, generation = 0, requestController = null;
    var manifest = null, validAt = 0, timer = null, expiryTimer = null, failureCount = 0;
    var suppressed = new Set(), loaded = new Set(), impressions = new Set(), nodes = new Map();
    var choices = new Map(), queue = [], flushing = false, observed = new Map();
    var touching = false, deferred = false, dialog = null, lastContext = '';
    var menu = null, dialogTrigger = null;
    var requestStarted = 0, inquiryNode = null, lastSideWidth = 0;
    function context() { return options.getContext(); }
    function inputActive() { return /^(INPUT|TEXTAREA|SELECT)$/.test((document.activeElement || {}).tagName || ''); }
    function keyboardOpen() {
      return window.innerWidth <= 768 && inputActive() && window.visualViewport && window.visualViewport.height < window.innerHeight - 120;
    }
    function key(item) { return item.campaign_id + ':' + item.id + ':' + item.placement; }
    function obscured(node) {
      if (dialog && dialog.open) return true;
      var box = node.getBoundingClientRect();
      var top = document.elementFromPoint(Math.max(0, Math.min(window.innerWidth - 1, box.left + box.width / 2)), Math.max(0, Math.min(window.innerHeight - 1, box.top + box.height / 2)));
      return !top || !node.contains(top);
    }
    function relayout() {
      var height = bottom.hidden ? 0 : bottom.getBoundingClientRect().height;
      var old = parseFloat(document.documentElement.style.getPropertyValue('--ad-reserved-height')) || 0;
      document.documentElement.style.setProperty('--ad-reserved-height', height + 'px');
      var main = right.parentElement;
      var sideStyle = getComputedStyle(right), item = right.querySelector('.ad-item');
      var itemStyle = item ? getComputedStyle(item) : null;
      function chrome(style, properties) {
        return style ? properties.reduce(function (total, property) { return total + (parseFloat(style[property]) || 0); }, 0) : 0;
      }
      // Height drives the image width; account for padding and borders on both axes.
      var verticalChrome = chrome(sideStyle, ['paddingTop', 'paddingBottom', 'borderTopWidth', 'borderBottomWidth']) + chrome(itemStyle, ['borderTopWidth', 'borderBottomWidth']);
      var inlineChrome = chrome(sideStyle, ['paddingLeft', 'paddingRight', 'borderLeftWidth', 'borderRightWidth']) + chrome(itemStyle, ['borderLeftWidth', 'borderRightWidth']);
      var sideHeight = main ? Math.max(0, main.getBoundingClientRect().height - verticalChrome) : 0;
      right.style.setProperty('--ad-side-image-height', sideHeight + 'px');
      right.style.setProperty('--ad-side-inline-chrome', inlineChrome + 'px');
      var sideWidth = right.hidden ? 0 : right.getBoundingClientRect().width;
      if ((old !== height || lastSideWidth !== sideWidth) && options.relayout) requestAnimationFrame(options.relayout);
      lastSideWidth = sideWidth;
    }
    function hide() {
      closeMenu(false);
      bottom.hidden = true; right.hidden = true;
      relayout();
    }
    function enqueue(type, item) {
      queue.push({ id: nonce(), type: type, campaign_id: item.campaign_id, creative_id: item.id, placement: item.placement, platform: context().platform });
      if (queue.length > 100) queue.splice(0, queue.length - 100);
    }
    function flush() {
      if (flushing || !queue.length || !authReady || !manifest || performance.now() >= validAt || !['shown', 'empty'].includes(manifest.reason)) return;
      flushing = true;
      var batch = queue.splice(0, 20);
      var startedGeneration = generation;
      var controller = new AbortController();
      var timeout = setTimeout(function () { controller.abort(); }, 5000);
      fetch(options.apiBase + '/api/ads/events', { method: 'POST', credentials: 'include', referrerPolicy: 'no-referrer', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ events: batch }), signal: controller.signal })
        .then(function (r) { if (!r.ok) throw new Error('events'); })
        .catch(function () { if (startedGeneration === generation && performance.now() < validAt) queue = batch.concat(queue).slice(0, 100); })
        .finally(function () { clearTimeout(timeout); flushing = false; });
    }
    function closeDialog(fromHistory) {
      if (!dialog) return;
      var wasOpen = dialog.open;
      dialog.close();
      if (wasOpen && dialogTrigger && dialogTrigger.isConnected && dialogTrigger.getClientRects().length) dialogTrigger.focus();
      if (wasOpen && !fromHistory && history.state && history.state.shuttleAdDialog) history.back();
    }
    function closeMenu(restoreFocus) {
      if (!menu) return;
      var trigger = menu.trigger;
      trigger.setAttribute('aria-expanded', 'false');
      trigger.removeAttribute('aria-controls');
      menu.node.remove(); menu = null;
      if (restoreFocus && trigger.isConnected && trigger.getClientRects().length) trigger.focus();
    }
    function positionMenu() {
      if (!menu) return;
      if (!menu.trigger.isConnected || !menu.trigger.getClientRects().length) { closeMenu(false); return; }
      var anchor = menu.trigger.getBoundingClientRect(), box = menu.node.getBoundingClientRect();
      var top = anchor.bottom + 4;
      if (top + box.height > window.innerHeight - 8) top = anchor.top - box.height - 4;
      menu.node.style.top = Math.max(8, Math.min(top, window.innerHeight - box.height - 8)) + 'px';
      menu.node.style.left = Math.max(8, Math.min(anchor.right - box.width, window.innerWidth - box.width - 8)) + 'px';
    }
    function showMenu(item, trigger) {
      if (menu && menu.trigger === trigger) { closeMenu(true); return; }
      closeMenu(false);
      var node = element('div', 'ad-menu'); node.id = 'shuttleAdMenu';
      node.setAttribute('role', 'dialog'); node.setAttribute('aria-label', '셔틀Go 광고');
      var feedback = element('button', 'ad-feedback-open', '의견 보내기'); feedback.type = 'button';
      function latest() { return manifest && manifest.candidates.find(function (candidate) { return key(candidate) === key(item); }) || item; }
      feedback.onclick = function () { showFeedback(latest(), trigger); };
      node.appendChild(feedback);
      // Keep targeting disclosure accessible in-app without a second menu action.
      node.appendChild(element('p', 'ad-selection-basis', '표시 기준: ' + latest().selection_basis.join(', ')));
      document.body.appendChild(node);
      menu = { node: node, trigger: trigger };
      trigger.setAttribute('aria-expanded', 'true'); trigger.setAttribute('aria-controls', node.id);
      positionMenu(); feedback.focus();
    }
    function openDialog(title, trigger) {
      closeMenu(false); dialogTrigger = trigger;
      if (!dialog) {
        dialog = element('dialog', 'ad-dialog');
        document.body.appendChild(dialog);
        dialog.addEventListener('cancel', function (event) { event.preventDefault(); closeDialog(false); });
        dialog.addEventListener('click', function (event) { if (event.target === dialog) { var b = dialog.getBoundingClientRect(); if (event.clientX < b.left || event.clientX > b.right || event.clientY < b.top || event.clientY > b.bottom) closeDialog(false); } });
      }
      dialog.replaceChildren();
      dialog.setAttribute('aria-label', title);
      var heading = element('div', 'ad-dialog-heading');
      heading.appendChild(element('h2', '', title));
      var close = element('button', 'ad-dialog-close', '×');
      close.type = 'button'; close.setAttribute('aria-label', '닫기'); close.onclick = function () { closeDialog(false); };
      heading.appendChild(close); dialog.appendChild(heading);
      history.pushState(Object.assign({}, history.state || {}, { shuttleAdDialog: true }), '', location.href);
    }
    function showFeedback(item, trigger) {
      openDialog('의견 보내기', trigger);
      var choices = element('div', 'ad-report-choices');
      var result = element('p', 'ad-report-result'); result.setAttribute('role', 'status');
      var reportId = nonce(), selected = null, busy = false;
      function send(choice, button) {
        if (busy) return;
        selected = selected || choice; busy = true;
        choices.querySelectorAll('button').forEach(function (node) { node.disabled = true; });
        result.textContent = '의견을 보내고 있습니다.';
        var controller = new AbortController(), timeout = setTimeout(function () { controller.abort(); }, 5000);
        // Preserve the existing report API; preset detail distinguishes the new feedback options.
        fetch(options.apiBase + '/api/ads/reports', { method: 'POST', credentials: 'omit', referrerPolicy: 'no-referrer', headers: { 'Content-Type': 'application/json' }, signal: controller.signal, body: JSON.stringify({ id: reportId, campaign_id: item.campaign_id, creative_id: item.id, reason: selected.reason, detail: selected.label }) })
          .then(function (r) { if (!r.ok) throw new Error('report'); suppressed.add(item.campaign_id); result.textContent = '의견이 접수되었습니다. 감사합니다.'; choices.hidden = true; render(); if (dialog.open && dialog.contains(choices)) dialog.querySelector('.ad-dialog-close').focus(); })
          .catch(function () { result.textContent = '의견을 보내지 못했습니다. 같은 버튼을 눌러 다시 보내거나 support@shuttle-go.com으로 문의해 주세요.'; busy = false; button.disabled = false; })
          .finally(function () { clearTimeout(timeout); });
      }
      [
        { reason: 'other', label: '콘텐츠를 가리는 광고' },
        { reason: 'inappropriate', label: '부적절한 광고' },
        { reason: 'age', label: '연령에 맞지 않는 광고' },
        { reason: 'other', label: '여러 번 표시된 광고' },
        { reason: 'other', label: '관심없는 광고' }
      ].forEach(function (choice) {
        var button = element('button', 'ad-report-choice', choice.label); button.type = 'button';
        button.onclick = function () { send(choice, button); }; choices.appendChild(button);
      });
      dialog.appendChild(choices); dialog.appendChild(result);
      dialog.showModal();
    }
    function createNode(item) {
      var node = element('article', 'ad-item'); node.dataset.adKey = key(item); node.setAttribute('aria-label', '셔틀Go 광고');
      var link = element('a', 'ad-link'); link.href = item.url; link.target = '_blank'; link.rel = 'noopener noreferrer sponsored'; link.referrerPolicy = 'no-referrer';
      var img = element('img'); img.alt = item.alt; img.referrerPolicy = 'no-referrer'; img.decoding = 'async';
      img.onload = function () { loaded.add(key(item)); };
      img.onerror = function () { suppressed.add(item.campaign_id); render(); };
      img.src = options.apiBase + item.image_url;
      link.appendChild(img);
      link.onclick = function (event) {
        if (!manifest || performance.now() >= validAt || !authReady) { event.preventDefault(); hide(); return; }
        enqueue('click', item); flush();
        if (options.openExternal && options.openExternal(item.url)) event.preventDefault();
      };
      node.appendChild(link);
      var info = element('button', 'ad-info'); info.type = 'button'; info.setAttribute('aria-label', '셔틀Go 광고'); info.setAttribute('aria-haspopup', 'dialog'); info.setAttribute('aria-expanded', 'false');
      var mark = element('span', 'ad-info-mark', 'ⓘ'); mark.setAttribute('aria-hidden', 'true');
      var tooltip = element('span', 'ad-info-tooltip', '셔틀Go 광고'); tooltip.setAttribute('aria-hidden', 'true');
      info.appendChild(mark); info.appendChild(tooltip);
      info.onclick = function () { if (!manifest || performance.now() >= validAt || !authReady) { hide(); return; } showMenu(item, info); };
      node.appendChild(info); nodes.set(key(item), node);
      observer.observe(node);
      return node;
    }
    var observer = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) { observed.set(entry.target, entry.intersectionRatio); });
    }, { threshold: [0, 0.5, 1] });
    function ranking(item) {
      if (!choices.has(key(item))) {
        // Independent page-local randomness: no cookie, user identifier or device ID.
        choices.set(key(item), Math.random());
      }
      return choices.get(key(item));
    }
    function inquiry() {
      if (inquiryNode) return inquiryNode;
      var node = element('a', 'ad-item ad-inquiry'); node.href = 'mailto:support@shuttle-go.com';
      node.appendChild(element('span', '', '광고 문의')); node.appendChild(element('span', '', 'support@shuttle-go.com'));
      inquiryNode = node;
      return node;
    }
    function render() {
      if (touching || document.getElementById('resultsPanel') && document.getElementById('resultsPanel').classList.contains('sheet-dragging')) { deferred = true; return; }
      var header = document.querySelector('.header');
      var availableHeight = window.innerHeight - (header ? header.getBoundingClientRect().height : 0) - (window.innerWidth <= 768 ? 120 : 156);
      if (!authReady || !manifest || document.hidden || performance.now() >= validAt || !['shown', 'empty'].includes(manifest.reason) || keyboardOpen() || availableHeight < 280 || window.innerHeight < (window.innerWidth <= 768 ? 500 : 480)) { hide(); return; }
      var width = window.innerWidth, maximum = capacity(width), mobile = width <= 768;
      var used = new Set(), selected = [];
      var candidates = manifest.candidates.filter(function (item) { return !suppressed.has(item.campaign_id); }).sort(function (a, b) { return b.priority - a.priority || ranking(a) - ranking(b); });
      // Reserve a distinct right-hand campaign before filling the bottom row.
      var side = mobile ? null : candidates.find(function (item) { return item.placement === 'desktop_right'; });
      if (side) used.add(side.campaign_id);
      candidates.forEach(function (item) {
        if (item.placement === (mobile ? 'mobile_bottom' : 'desktop_bottom') && selected.length < maximum && !used.has(item.campaign_id)) { selected.push(item); used.add(item.campaign_id); }
      });
      var desired = selected.map(function (item) { return nodes.get(key(item)) || createNode(item); });
      if (manifest.inquiry && desired.length < maximum) desired.push(inquiry());
      function reconcile(parent, children) {
        Array.from(parent.children).forEach(function (node) { if (!children.includes(node)) node.remove(); });
        children.forEach(function (node, index) { if (parent.children[index] !== node) parent.insertBefore(node, parent.children[index] || null); });
      }
      // Reuse nodes so polling does not reload images, reset focus, or recount impressions.
      reconcile(bottom, desired); bottom.hidden = !desired.length;
      bottom.classList.toggle('ads-empty', !selected.length);
      bottom.style.setProperty('--ads-columns', desired.length);
      reconcile(right, side ? [nodes.get(key(side)) || createNode(side)] : []); right.hidden = !side;
      relayout(); positionMenu();
    }
    function invalidate() {
      generation += 1;
      if (requestController) requestController.abort();
      requestController = null; clearTimeout(timer); clearTimeout(expiryTimer); queue = []; manifest = null; validAt = 0;
      hide();
    }
    function schedule(delay) { clearTimeout(timer); if (!document.hidden && authReady) timer = setTimeout(refresh, delay); }
    function refresh() {
      if (!authReady || document.hidden) return;
      var ctx = context(), ctxKey = ctx.siteId + '|' + ctx.platform + '|' + (window.innerWidth <= 768 ? 'mobile' : 'desktop');
      if (ctxKey !== lastContext) { invalidate(); lastContext = ctxKey; }
      if (requestController) requestController.abort();
      var currentGeneration = ++generation, controller = new AbortController(); requestController = controller;
      requestStarted = performance.now();
      var timeout = setTimeout(function () { controller.abort(); }, 5000);
      var query = new URLSearchParams({ site_id: ctx.siteId || '', platform: ctx.platform || 'web', layout: window.innerWidth <= 768 ? 'mobile' : 'desktop' });
      fetch(options.apiBase + '/api/ads?' + query, { credentials: 'include', referrerPolicy: 'no-referrer', signal: controller.signal })
        .then(function (r) { if (!r.ok) throw new Error('manifest'); return r.json(); })
        .then(function (data) {
          if (currentGeneration !== generation) return;
          if (data.schema_version !== 1 || !Array.isArray(data.candidates) || !Number.isFinite(Date.parse(data.server_time)) || !Number.isFinite(Date.parse(data.valid_until)) || manifest && data.revision < manifest.revision) throw new Error('schema');
          var validity = Math.min(90000, Date.parse(data.valid_until) - Date.parse(data.server_time));
          manifest = data; validAt = requestStarted + Math.max(0, validity); failureCount = 0;
          var liveKeys = new Set(data.candidates.map(key));
          nodes.forEach(function (node, id) { if (!liveKeys.has(id)) { observer.unobserve(node); observed.delete(node); node.remove(); nodes.delete(id); loaded.delete(id); } });
          clearTimeout(expiryTimer); expiryTimer = setTimeout(function () { hide(); refresh(); }, Math.max(0, validAt - performance.now()));
          render(); schedule(30000);
        })
        .catch(function () { if (currentGeneration !== generation) return; failureCount += 1; if (!manifest || performance.now() >= validAt) hide(); schedule(Math.min(60000, 5000 * Math.pow(2, Math.min(failureCount - 1, 4))) + Math.random() * 1000); })
        .finally(function () { clearTimeout(timeout); if (currentGeneration === generation) requestController = null; });
    }
    var visibleSince = new Map();
    setInterval(function () {
      if (!manifest || !authReady || performance.now() >= validAt || document.hidden) { visibleSince.clear(); return; }
      observed.forEach(function (ratio, node) {
        var id = node.dataset.adKey;
        if (!node.isConnected || !loaded.has(id) || ratio < 0.5 || obscured(node)) { visibleSince.delete(id); return; }
        if (impressions.has(id)) return;
        if (!visibleSince.has(id)) visibleSince.set(id, performance.now());
        if (performance.now() - visibleSince.get(id) >= 1000) {
          var item = manifest.candidates.find(function (item) { return key(item) === id; });
          if (item) { impressions.add(id); enqueue('impression', item); }
        }
      });
      if (deferred && !touching) { deferred = false; render(); }
    }, 250);
    setInterval(flush, 3000);
    var resizeTimer;
    window.addEventListener('resize', function () { clearTimeout(resizeTimer); resizeTimer = setTimeout(function () { render(); if (authReady) refresh(); }, 180); });
    if (window.visualViewport) window.visualViewport.addEventListener('resize', render);
    document.addEventListener('focusin', function (event) { if (menu && !menu.node.contains(event.target) && event.target !== menu.trigger) closeMenu(false); render(); }); document.addEventListener('focusout', function () { setTimeout(render, 100); });
    document.addEventListener('pointerdown', function (event) { touching = true; if (menu && !menu.node.contains(event.target) && !menu.trigger.contains(event.target)) closeMenu(false); }, { passive: true });
    document.addEventListener('keydown', function (event) { if (menu && event.key === 'Escape') { event.preventDefault(); event.stopImmediatePropagation(); closeMenu(true); } }, true);
    document.addEventListener('scroll', positionMenu, true);
    ['pointerup', 'pointercancel'].forEach(function (name) { document.addEventListener(name, function () { touching = false; if (deferred) { deferred = false; render(); } }, { passive: true }); });
    document.addEventListener('visibilitychange', function () { if (document.hidden) { clearTimeout(timer); hide(); visibleSince.clear(); } else { render(); refresh(); } });
    window.addEventListener('focus', function () { if (authReady && !document.hidden) refresh(); });
    window.addEventListener('pageshow', function () { if (authReady) refresh(); });
    window.addEventListener('popstate', function () { closeMenu(false); if (dialog && dialog.open) closeDialog(true); });
    var priorBack = window.ShuttleGoAppBackHandler;
    window.ShuttleGoAppBackHandler = { handleBack: function () { if (dialog && dialog.open) { closeDialog(false); return true; } if (menu) { closeMenu(true); return true; } return !!(priorBack && priorBack.handleBack && priorBack.handleBack()); } };
    if (window.ResizeObserver) {
      var layoutObserver = new ResizeObserver(relayout);
      layoutObserver.observe(bottom);
      if (right.parentElement) layoutObserver.observe(right.parentElement);
    }
    return { setAuthReady: function (ready) { invalidate(); authReady = !!ready; if (ready) refresh(); }, contextChanged: function () { invalidate(); if (authReady) refresh(); }, refresh: refresh, reservedHeight: function () { return bottom.hidden ? 0 : bottom.getBoundingClientRect().height; } };
  }
  window.ShuttleAds = { start: start, capacity: capacity };
}());
