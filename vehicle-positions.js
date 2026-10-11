(function(global) {
  'use strict';
  function node(tag, className, text) {
    var el = document.createElement(tag);
    el.className = className || '';
    if (text != null) el.textContent = String(text);
    return el;
  }
  function clock(value) {
    var date = new Date(value);
    if (!isFinite(date.getTime())) return '';
    return new Intl.DateTimeFormat('ko-KR', { hour: '2-digit', minute: '2-digit', hourCycle: 'h23', timeZone: 'Asia/Seoul' }).format(date);
  }
  function occupancyText(value) {
    if (!value) return '';
    var count = Number.isInteger(value.onboard_count) ? value.onboard_count : null;
    var capacity = Number.isInteger(value.capacity) && value.capacity > 0 ? value.capacity : null;
    return count == null && capacity == null ? '' : '탑승 ' + (count == null ? '–' : count)
      + (capacity == null ? '' : '/' + capacity) + '명';
  }
  function retryAfter(response) {
    var raw = response.headers.get('Retry-After');
    if (!raw) return 0;
    return Math.max(0, /^[0-9]+$/.test(raw) ? Number(raw) * 1000 : Date.parse(raw) - Date.now()) || 0;
  }
  function start(options) {
    var generation = 0, entries = [], targets = [], overlays = {}, config = null, suspended = false;
    var mapListenerTarget = null;
    var countdownTimer = null, idleTimer = null, expiryTimer = null, idlePaused = false;
    var reasonTooltip = null, reasonTrigger = null;
    var statusViews = new Map();
    var lastActivity = null, lastActivityWall = null, lastSignal = -Infinity;
    var idleTimeout = 300000;
    var etaRetentionMs = 90000;
    var markerSizeCache = new Map();
    var picker = null, pickerState = null;
    function selectionScope(target, serviceDate, revision) {
      var c = target.context;
      return JSON.stringify([c.route_id,c.route_uuid,c.site_id,c.day_type,c.target_stop_uuid,
        serviceDate,revision,c.historical_snapshot,c.shared_route_token]);
    }
    function currentOwners(vehicle) {
      return (vehicle.owners || []).filter(function(owner) {
        return owner.generation === generation && entries.indexOf(owner.entry) >= 0 &&
          owner.target.card.isConnected && owner.target.card.classList.contains('selected');
      });
    }
    function canResumeVehicle(vehicle) { return idlePaused && currentOwners(vehicle).length>0; }
    function selectionFor(vehicle) {
      if (!vehicle.vehicle_id) return null;
      var owners = currentOwners(vehicle).filter(function(owner) { return owner.target.departureSelectable===true; });
      if (!owners.length || !options.onSelectDeparture ||
          owners.some(function(owner) { return owner.scope !== owners[0].scope || owner.target.option !== owners[0].target.option; })) return null;
      var first = owners[0];
      var selection = {target:first.target,vehicleId:vehicle.vehicle_id,departureTime:vehicle.departure_time,
        serviceDate:first.serviceDate,routeRevision:first.revision};
      return !options.canSelectDeparture || options.canSelectDeparture(selection) ? selection : null;
    }
    function selectVehicle(vehicle) {
      var selection=selectionFor(vehicle);
      if (canResumeVehicle(vehicle) && !resume()) return;
      if (selection) options.onSelectDeparture(selection);
    }
    function activateGroup(key) {
      var group=overlays[key] && overlays[key].group;
      if (!group) return;
      if (group.vehicles.length===1) { selectVehicle(group.vehicles[0]); return; }
      if (group.vehicles.some(canResumeVehicle)) {
        if (!resume()) return;
        group=overlays[key] && overlays[key].group;
        if (!group) return;
      }
      if (pickerState && pickerState.key===key) { closePicker(); return; }
      var selectable=group.vehicles.map(selectionFor).find(Boolean);
      if (!selectable) return;
      closePicker(); pickerState={key:key,option:selectable.target.option,context:selectable.target.context,
        scope:selectionScope(selectable.target,selectable.serviceDate,selectable.routeRevision)};
      overlays[key].root.querySelector('button').setAttribute('aria-expanded','true'); renderPicker(group);
    }
    function closePicker(restoreFocus) {
      var previous = pickerState && overlays[pickerState.key];
      if (picker) { picker.remove(); picker = null; }
      pickerState = null;
      Object.keys(overlays).forEach(function(key) {
        var button = overlays[key].root && overlays[key].root.querySelector('[aria-expanded]');
        if (button) button.setAttribute('aria-expanded','false');
      });
      if (restoreFocus && previous && previous.root.isConnected) {
        var trigger=previous.root.querySelector('button');
        if (trigger) trigger.focus({preventScroll:true});
      }
    }
    function markerInteraction() { if (options.onMarkerInteraction) options.onMarkerInteraction(); }
    function bindMarkerButton(button, activate) {
      var pointer = null, moved = false, started = 0, touchHandledAt = -Infinity;
      button.addEventListener('pointerdown',function(event) {
        moved = false; started = activityTime(); pointer = {x:event.clientX,y:event.clientY,id:event.pointerId};
      });
      function track(event) {
        if (pointer && event.pointerId === pointer.id && Math.hypot(event.clientX-pointer.x,event.clientY-pointer.y) >= 8) moved = true;
      }
      button.addEventListener('pointermove',track);
      button.addEventListener('pointerup',track);
      button.addEventListener('pointercancel',function() { moved = true; });
      button.addEventListener('touchstart',function(event) {
        var touch=event.touches[0];
        moved=event.touches.length!==1; started=activityTime();
        pointer=touch ? {x:touch.clientX,y:touch.clientY} : null;
      },{passive:true});
      button.addEventListener('touchmove',function(event) {
        var touch=event.touches[0];
        if (event.touches.length!==1 || pointer && touch && Math.hypot(touch.clientX-pointer.x,touch.clientY-pointer.y)>=8) moved=true;
      },{passive:true});
      button.addEventListener('touchcancel',function() { moved=true; touchHandledAt=activityTime(); });
      function allowed() {
        return button.isConnected && !moved && activityTime()-started<=600 &&
          !(options.mapDraggedRecently && options.mapDraggedRecently());
      }
      // Kakao handles touchend before a compatibility click on some WebViews.
      // Consume the tap here and suppress its later click, while keeping native keyboard clicks.
      button.addEventListener('touchend',function(event) {
        event.preventDefault(); event.stopPropagation(); markerInteraction(); touchHandledAt=activityTime();
        if (allowed()) activate();
      },{passive:false});
      button.addEventListener('click',function(event) {
        event.preventDefault(); event.stopPropagation(); markerInteraction();
        if (event.detail && activityTime()-touchHandledAt<800) return;
        if (!button.isConnected || (event.detail && (moved || activityTime()-started>600)) ||
            options.mapDraggedRecently && options.mapDraggedRecently()) return;
        activate();
      });
    }
    function appendVehicleLines(list, vehicle) {
      list.appendChild(node('span','live-vehicle-line',vehicle.departure_time + (vehicle.plate_suffix ? ' · ' + vehicle.plate_suffix : '')));
      var detailText = occupancyText(vehicle.occupancy), detail = node('span','live-vehicle-line');
      if (detailText) detail.appendChild(node('span','',detailText));
      if (vehicle.live_eta) {
        var estimate = node('small','',(detailText ? ' · ' : '') + '예상 ' + clock(vehicle.live_eta.eta_at));
        if (vehicle.eta_retained) estimate.title = '최근 계산한 도착 예상시각입니다.';
        detail.appendChild(estimate);
      }
      if (detail.childNodes.length) list.appendChild(detail);
    }
    function positionPicker() {
      if (!picker || !pickerState) return;
      var overlay = overlays[pickerState.key];
      if (!overlay || !overlay.root.isConnected) return;
      var anchor = overlay.root.getBoundingClientRect(), margin = 10;
      picker.style.maxWidth = Math.min(320,global.innerWidth-margin*2)+'px';
      picker.style.maxHeight = Math.min(360,global.innerHeight-margin*2)+'px';
      var bounds = picker.getBoundingClientRect();
      picker.style.left = Math.max(margin,Math.min(anchor.left,global.innerWidth-bounds.width-margin))+'px';
      var top = anchor.bottom+6;
      if (top+bounds.height>global.innerHeight-margin) top=anchor.top-bounds.height-6;
      picker.style.top = Math.max(margin,Math.min(top,global.innerHeight-bounds.height-margin))+'px';
    }
    function renderPicker(group) {
      if (!pickerState) return;
      if (!group) {
        if (entries.length && entries.every(function(entry) { return entryState(entry)==='loading'; })) {
          if (picker) picker.querySelectorAll('button').forEach(function(button) { button.disabled=true; });
          return;
        }
        closePicker(); return;
      }
      if (!picker) {
        picker = node('div','live-vehicle-picker'); picker.setAttribute('role','dialog');
        picker.setAttribute('aria-label','버스 선택'); document.body.appendChild(picker);
      }
      var focused = picker.contains(document.activeElement) ? document.activeElement.getAttribute('data-vehicle-key') : null;
      var scrollTop = picker.scrollTop;
      picker.replaceChildren();
      group.vehicles.forEach(function(vehicle) {
        var selection = selectionFor(vehicle), row = node(selection ? 'button' : 'div','live-vehicle-choice'+
          (vehicle.selected_trip ? ' is-selected' : '')+(vehicle.display_stale ? ' is-stale' : ''));
        row.setAttribute('data-vehicle-key',vehicle.markerKey);
        if (selection) {
          row.type='button'; row.setAttribute('aria-pressed',String(!!vehicle.selected_trip));
          row.setAttribute('aria-label',vehicle.departure_time+' 출발 버스 선택'+(vehicle.plate_suffix ? ' · '+vehicle.plate_suffix : ''));
          bindMarkerButton(row,function() { selectVehicle(vehicle); });
        }
        var list=node('span','live-vehicle-lines'); appendVehicleLines(list,vehicle); row.appendChild(list); picker.appendChild(row);
        if (focused === vehicle.markerKey && selection) row.focus({preventScroll:true});
      });
      picker.scrollTop=scrollTop; positionPicker();
    }
    document.addEventListener('click',function(event) {
      if (!pickerState || picker && picker.contains(event.target) || event.target.closest && event.target.closest('.live-vehicle-marker')) return;
      var container=options.getMapContainer && options.getMapContainer();
      if (container && container.contains(event.target)) markerInteraction();
      closePicker();
    },true);
    document.addEventListener('keydown',function(event) { if (event.key==='Escape') closePicker(true); });
    global.addEventListener('resize',positionPicker);
    document.addEventListener('scroll',positionPicker,true);
    function mapGesture() { closePicker(); renderMap(); }
    function markerBounds(vehicle) {
      var first = vehicle.departure_time + (vehicle.plate_suffix ? ' · ' + vehicle.plate_suffix : '');
      var detail = [occupancyText(vehicle.occupancy), vehicle.live_eta ? '예상 ' + clock(vehicle.live_eta.eta_at) : ''].filter(Boolean).join(' · ');
      var key = JSON.stringify([first,detail,!!vehicle.selected_trip]);
      if (markerSizeCache.has(key)) return markerSizeCache.get(key);
      var root = node('div','live-vehicle-marker');
      root.style.cssText = 'position:fixed;visibility:hidden;pointer-events:none;width:max-content;left:0;top:0;';
      var label = node('div','live-vehicle-label' + (vehicle.selected_trip ? ' is-selected' : ''));
      var image = node('img'); image.width = 20; image.height = 20; label.appendChild(image);
      var lines = node('div','live-vehicle-lines');
      lines.appendChild(node('div','live-vehicle-line',first));
      if (detail) lines.appendChild(node('div','live-vehicle-line',detail));
      label.appendChild(lines); root.appendChild(label); document.body.appendChild(root);
      var rect = root.getBoundingClientRect(); root.remove();
      var size = {width:rect.width || 150,height:rect.height || 50};
      if (markerSizeCache.size >= 64) markerSizeCache.clear();
      markerSizeCache.set(key,size);
      return size;
    }
    global.addEventListener('resize',function() { markerSizeCache.clear(); renderMap(); });
    if (document.fonts && document.fonts.ready) document.fonts.ready.then(function() { markerSizeCache.clear(); renderMap(); });
    function activityTime() { return global.performance.now(); }
    function hideReason() {
      if (reasonTrigger) reasonTrigger.setAttribute('aria-expanded','false');
      reasonTrigger = null;
      if (reasonTooltip) reasonTooltip.hidden = true;
    }
    function positionReason() {
      if (!reasonTrigger || !reasonTrigger.isConnected) { hideReason(); return; }
      var box = reasonTrigger.getBoundingClientRect(), margin = 10;
      reasonTooltip.style.maxWidth = Math.min(320,global.innerWidth-margin*2) + 'px';
      var size = reasonTooltip.getBoundingClientRect();
      reasonTooltip.style.left = Math.max(margin,Math.min(box.right-size.width,global.innerWidth-size.width-margin)) + 'px';
      reasonTooltip.style.top = Math.max(margin,Math.min(box.bottom+8+size.height>global.innerHeight-margin ? box.top-size.height-8 : box.bottom+8,global.innerHeight-size.height-margin)) + 'px';
    }
    function showReason(trigger,text) {
      hideReason();
      if (!reasonTooltip) {
        reasonTooltip = node('div','route-live-reason-tooltip');
        reasonTooltip.id = 'route-live-reason-' + Array.from(global.crypto.getRandomValues(new Uint32Array(2))).join('-');
        reasonTooltip.setAttribute('role','tooltip');
        document.body.appendChild(reasonTooltip);
      }
      reasonTrigger = trigger;
      trigger.setAttribute('aria-describedby',reasonTooltip.id);
      trigger.setAttribute('aria-expanded','true');
      reasonTooltip.textContent = text; reasonTooltip.hidden = false; positionReason();
    }
    function reasonText(reason) {
      return {
        different_service_date:'오늘 운행하는 버스만 확인할 수 있습니다.',
        different_day_type:'오늘 운행하는 버스가 없습니다.',
        historical_snapshot:'현재 노선으로 다시 검색해 주세요.',
        route_unavailable:'이 노선의 버스 위치를 확인할 수 없습니다.',
        route_changed:'노선이 변경되었습니다. 다시 검색해 주세요.',
        stop_unavailable:'정류장이 변경되었습니다. 다시 검색해 주세요.',
        departure_unavailable:'이 출발 시간에는 운행하는 버스가 없습니다.',
        source_unavailable:'이 노선은 버스 위치를 제공하지 않습니다.',
        too_many_views:'잠시 후 버스 위치를 다시 확인합니다.',
        feature_disabled:'버스 위치 안내를 제공하지 않습니다.',
        budget_paused:'버스 위치 갱신이 잠시 중단되었습니다.',
        usage_unknown:'버스 위치 갱신이 잠시 중단되었습니다.',
        timeout:'응답이 늦어지고 있습니다. 잠시 후 다시 확인합니다.',
        network_error:'연결이 원활하지 않습니다. 잠시 후 다시 확인합니다.',
        service_unavailable:'버스 위치를 불러오지 못했습니다. 잠시 후 다시 확인합니다.'
      }[reason] || '버스 위치를 확인할 수 없습니다. 다시 검색해 주세요.';
    }
    function addReason(view,text) {
      var button = node('button','route-live-info');
      button.type = 'button'; button.setAttribute('aria-label','버스 위치 안내'); button.setAttribute('aria-expanded','false');
      var icon = node('img'); icon.src = '/images/lucide-info.svg'; icon.alt = ''; button.appendChild(icon);
      var fine = function() { return global.matchMedia('(hover: hover) and (pointer: fine)').matches; };
      button.addEventListener('mouseenter',function() { if (fine()) showReason(button,text); });
      button.addEventListener('mouseleave',function() { if (fine() && reasonTrigger===button) hideReason(); });
      button.addEventListener('focus',function() { if (fine()) showReason(button,text); });
      button.addEventListener('blur',function() { if (reasonTrigger===button) hideReason(); });
      button.addEventListener('click',function(event) {
        event.preventDefault(); event.stopPropagation();
        if (reasonTrigger===button && !reasonTooltip.hidden && !fine()) hideReason(); else showReason(button,text);
      });
      view.row.appendChild(button);
    }
    document.addEventListener('click',function(event) { if (!event.target || !event.target.closest || !event.target.closest('.route-live-info')) hideReason(); });
    document.addEventListener('keydown',function(event) { if (event.key==='Escape') hideReason(); });
    global.addEventListener('resize',positionReason);
    document.addEventListener('scroll',function() { if (reasonTrigger) positionReason(); },true);
    function contextKey(target, positionsOnly) {
      var c = target.context;
      return JSON.stringify(['route_id','route_uuid','site_id','day_type','departure_time','service_date','route_revision','target_stop_uuid','historical_snapshot','shared_route_token']
        .filter(function(k) { return !positionsOnly || k !== 'departure_time'; }).map(function(k) { return c[k] == null ? null : c[k]; }));
    }
    function positionHandoff(entry, target, resuming) {
      if (!entry || entry.target.option !== target.option || contextKey(entry.target,true) !== contextKey(target,true) ||
          (resuming ? contextKey(entry.target)!==contextKey(target) : !target.departureSelectable || !entry.target.departureSelectable)) return null;
      var data = entry.data, at = entry.receivedAt;
      if ((!data || !data.vehicles.length) && entry.handoff) { data=entry.handoff.data; at=entry.handoff.at; }
      if (data && data.state==='unavailable' && !data.vehicles.length && entry.lastGood &&
          data.route_revision===entry.lastGood.route_revision && data.live_service_date===entry.lastGood.live_service_date) {
        data=entry.lastGood; at=entry.lastGoodAt;
      }
      var today = new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Seoul'}).format(new Date());
      var date = target.context.service_date || today;
      if (!data || !data.vehicles.length || !data.route_revision || date!==today || data.live_service_date!==date ||
          target.context.route_revision && target.context.route_revision!==data.route_revision) return null;
      // Keep the original age clock. Selecting another departure never refreshes a GPS fix.
      return {data:data,at:at};
    }
    function previewDeparture(option, departureTime) {
      entries.forEach(function(entry) {
        if (entry.target.option!==option || !entry.target.departureSelectable) return;
        entry.previewDeparture=departureTime || null;
        if (entry.previewDeparture && entry.previewDeparture!==entry.target.context.departure_time) entry.predictions={};
      });
      renderMap();
    }
    function idleElapsed() { return lastActivity == null ? 0 : Math.max(activityTime()-lastActivity,Date.now()-lastActivityWall,0); }
    function idleExpired() { return lastActivity != null && idleElapsed() >= idleTimeout; }
    function armIdle() {
      clearTimeout(idleTimer);
      if (!idlePaused && entries.length && lastActivity != null) idleTimer = setTimeout(pauseIdle, Math.max(0, idleTimeout - idleElapsed()));
    }
    function renderIdle(view) {
      view.row.removeAttribute('title'); view.statusState = 'idle';
      view.row.setAttribute('aria-busy','false');
      view.row.replaceChildren(node('span', 'route-live-separator', ' · '));
      var button = node('button', 'route-live-resume');
      button.type = 'button'; button.title = '버스 위치 확인 재개';
      var ring = node('span', 'route-live-spinner is-paused');
      var icon = node('img', 'route-live-play'); icon.src = '/images/lucide-play.svg'; icon.alt = ''; ring.appendChild(icon);
      button.appendChild(node('span', 'route-live-status', '버스 위치 확인 재개'));
      button.appendChild(ring);
      button.addEventListener('click', function(event) { event.preventDefault(); event.stopPropagation(); resume(); });
      view.row.appendChild(button); view.counter = null;
    }
    function pauseIdle() {
      if (!entries.length || idlePaused) return;
      generation += 1; idlePaused = true;
      clearTimeout(idleTimer); clearInterval(countdownTimer); countdownTimer = null;
      var reason = 'idle';
      entries.forEach(function(entry) { clearEntry(entry, reason); });
      renderRows();
      renderMap();
    }
    function resume() {
      if (!idlePaused || document.hidden || suspended || !targets.length) return false;
      var next = targets.slice();
      idlePaused = false; lastActivity = activityTime(); lastActivityWall = Date.now();
      select(next,true);
      return true;
    }
    function activity(event) {
      if (!event.isTrusted || document.hidden || suspended || (event.type === 'pointermove' && !event.buttons)) return;
      lastSignal = activityTime();
      if (!idlePaused && targets.length) { lastActivity = lastSignal; lastActivityWall = Date.now(); armIdle(); }
    }
    ['pointerdown','pointermove','touchstart','wheel','keydown'].forEach(function(name) {
      document.addEventListener(name, activity, {capture:true,passive:true});
    });
    var configPromise = fetch(options.apiBase + '/api/vehicle-positions/config', { credentials: 'include' })
      .then(function(r) { return r.ok ? r.json() : {}; })
      .then(function(data) { config = data; return data; }).catch(function() { config = {}; return {}; });
    function removeMap() {
      Object.keys(overlays).forEach(function(key) { overlays[key].overlay.setMap(null); });
      overlays = {};
    }
    function clearEntry(entry, reason) {
      hideReason();
      entry.pending = false;
      clearTimeout(entry.timer);
      if (entry.abort) entry.abort.abort();
      if (entry.watch) {
        fetch(options.apiBase + '/api/vehicle-positions/watch/' + encodeURIComponent(entry.watch.watch_id) + '/close', {
          method: 'POST', credentials: 'include', keepalive: true,
          headers: { 'X-Shuttle-Position-Capability': entry.watch.capability, 'Content-Type':'application/json' },
          body:JSON.stringify({reason:reason || 'view_change'})
        }).catch(function() {});
        entry.watch = null;
      }
    }
    function stop(keepTargets, reason, keepPositions) {
      generation += 1;
      if (!keepTargets || reason && reason !== 'view_change') closePicker();
      entries.forEach(function(entry) { clearEntry(entry, reason || 'view_change'); });
      entries = [];
      statusViews.forEach(function(view) { view.row.remove(); });
      statusViews.clear();
      clearInterval(countdownTimer);
      clearTimeout(idleTimer); clearTimeout(expiryTimer);
      countdownTimer = null;
      if (!keepPositions) removeMap();
      if (mapListenerTarget && global.kakao && global.kakao.maps) {
        global.kakao.maps.event.removeListener(mapListenerTarget, 'zoom_changed', mapGesture);
        global.kakao.maps.event.removeListener(mapListenerTarget, 'dragstart', mapGesture);
      }
      mapListenerTarget = null;
      if (!keepTargets) { targets = []; idlePaused = false; lastActivity = null; lastActivityWall = null; }
    }
    function renderMap() {
      var map = options.getMap();
      clearTimeout(expiryTimer);
      if (!config || !config.enabled || !map || !global.kakao || !global.kakao.maps || document.hidden || suspended) { closePicker(); removeMap(); return; }
      if (mapListenerTarget !== map) {
        if (mapListenerTarget) {
          global.kakao.maps.event.removeListener(mapListenerTarget, 'zoom_changed', mapGesture);
          global.kakao.maps.event.removeListener(mapListenerTarget, 'dragstart', mapGesture);
        }
        global.kakao.maps.event.addListener(map, 'zoom_changed', mapGesture);
        global.kakao.maps.event.addListener(map, 'dragstart', mapGesture);
        mapListenerTarget = map;
      }
      var groups = [], projection = map.getProjection(), nextExpiry = Infinity;
      var updated = {}, now = Date.now();
      var seenVehicles = {}, uniqueVehicles = [];
      entries.forEach(function(entry) {
        if (!entry.target.card.isConnected || !entry.target.card.classList.contains('selected')) return;
        var display = entry.data, displayAt = entry.receivedAt;
        if (display && display.state === 'unavailable' && !display.vehicles.length && entry.lastGood
            && display.route_revision === entry.lastGood.route_revision && display.live_service_date === entry.lastGood.live_service_date) { display = entry.lastGood; displayAt = entry.lastGoodAt; }
        var handedOff = (!display || !display.vehicles.length) && entry.handoff;
        if (handedOff) { display=entry.handoff.data; displayAt=entry.handoff.at; }
        var previewing = !!entry.previewDeparture && entry.previewDeparture!==entry.target.context.departure_time;
        (display && display.vehicles || []).forEach(function(vehicle) {
          var observed = new Date(vehicle.observed_at).getTime(), age = now - observed;
          if (typeof vehicle.gps_age_seconds === 'number' && isFinite(vehicle.gps_age_seconds) && displayAt != null) {
            if (vehicle.gps_age_seconds < 0 || vehicle.gps_age_seconds > 180) return;
            age = vehicle.gps_age_seconds * 1000 + Math.max(0,activityTime()-displayAt);
          }
          var etaKey = predictionKey(entry,display,vehicle);
          if (age>60000 && entry.predictions) delete entry.predictions[etaKey];
          if (!isFinite(observed) || age > 180000) return;
          nextExpiry = Math.min(nextExpiry, now + 180001 - age);
          if (!idlePaused && vehicle.live_eta && age <= 60000) nextExpiry = Math.min(nextExpiry, now + 60001 - age);
          var selected = vehicle.departure_time===(entry.previewDeparture || entry.target.context.departure_time);
          var eta = !handedOff && !previewing && selected && !idlePaused && age <= 60000 ? vehicle.live_eta : null, held = false;
          var memory = entry.predictions && entry.predictions[etaKey];
          if (!eta && memory && !handedOff && !previewing) {
            var rememberedAge = memory.age + Math.max(0,activityTime()-memory.at);
            var etaAt = new Date(memory.eta.eta_at).getTime();
            var temporary = ['warming_up','off_route','ambiguous_position','previous_position_unmatched','backward_progress','profile_missing'].includes(vehicle.live_eta_state);
            if (!idlePaused && selected && age<=60000 && temporary && rememberedAge<etaRetentionMs && etaAt>now) {
              eta = memory.eta; held = true;
              nextExpiry = Math.min(nextExpiry,now+etaRetentionMs-rememberedAge,etaAt);
            }
          }
          vehicle = Object.assign({}, vehicle, {live_eta:eta,eta_retained:held,selected_trip:selected,
            display_stale: idlePaused || age > 60000 || vehicle.position_state === 'stale'});
          var occupancy = vehicle.occupancy;
          if (occupancy) {
            var occupancyAge = typeof vehicle.occupancy_age_seconds === 'number' && displayAt != null
              ? vehicle.occupancy_age_seconds * 1000 + Math.max(0,activityTime()-displayAt)
              : now-new Date(occupancy.received_at).getTime();
            if (!isFinite(occupancyAge) || occupancyAge > 60000 || occupancyAge < -5000) vehicle.occupancy = null;
            else nextExpiry = Math.min(nextExpiry, now+60001-Math.max(0,occupancyAge));
          }
          var lat = Number(vehicle.lat), lon = Number(vehicle.lon);
          if (!isFinite(lat) || !isFinite(lon)) return;
          var owner = {entry:entry,target:entry.target,generation:generation,serviceDate:display.live_service_date,
            revision:display.route_revision,scope:selectionScope(entry.target,display.live_service_date,display.route_revision)};
          vehicle.owners=[owner];
          var identity = vehicle.vehicle_id ? JSON.stringify([vehicle.vehicle_id,vehicle.departure_time,display.live_service_date]) : '';
          vehicle.markerKey=identity || JSON.stringify([vehicle.departure_time,vehicle.plate_suffix,lat,lon,display.live_service_date]);
          if (identity && seenVehicles[identity]) {
            var prior = seenVehicles[identity], selected = prior.selected_trip || vehicle.selected_trip, owners = prior.owners.concat(vehicle.owners);
            if (new Date(vehicle.observed_at).getTime() > new Date(prior.observed_at).getTime()) Object.assign(prior, vehicle);
            if (vehicle.occupancy && (!prior.occupancy || new Date(vehicle.occupancy.received_at) > new Date(prior.occupancy.received_at))) prior.occupancy = vehicle.occupancy;
            prior.selected_trip = selected;
            prior.owners = owners;
            if (vehicle.selected_trip && vehicle.live_eta) { prior.live_eta = vehicle.live_eta; prior.eta_retained = vehicle.eta_retained; }
            if (owners.some(function(item) { return item.scope !== owners[0].scope; })) { prior.live_eta=null; prior.eta_retained=false; }
            return;
          }
          if (identity) seenVehicles[identity] = vehicle;
          uniqueVehicles.push(vehicle);
        });
      });
      uniqueVehicles.forEach(function(vehicle) {
        var point = projection.containerPointFromCoords(new global.kakao.maps.LatLng(vehicle.lat, vehicle.lon));
        var bounds = markerBounds(vehicle);
        var group = groups.find(function(g) { return Math.abs(g.x - point.x) < (g.width + bounds.width) / 2
          && Math.abs(g.y - point.y) < Math.max(g.height, bounds.height); });
        if (group) group.vehicles.push(vehicle);
        else groups.push({x:point.x,y:point.y,width:bounds.width,height:bounds.height,vehicles:[vehicle]});
      });
      var pickerGroup = null;
      groups.forEach(function(group) {
        var vehicles = group.vehicles.slice().sort(function(a,b) { return String(a.vehicle_id || a.departure_time + a.plate_suffix).localeCompare(String(b.vehicle_id || b.departure_time + b.plate_suffix)); });
        var first = vehicles.find(function(v) { return v.selected_trip; }) || vehicles[0];
        group.vehicles=vehicles;
        var key = vehicles.map(function(v) { return v.markerKey; }).join('|');
        group.key=key;
        if (pickerState && pickerState.key===key && vehicles.length>1 && vehicles.some(function(v) {
          var selection=selectionFor(v);
          return selection && selectionScope(selection.target,selection.serviceDate,selection.routeRevision)===pickerState.scope;
        })) pickerGroup=group;
        var selected = vehicles.some(function(v) { return v.selected_trip; });
        var stale = vehicles.some(function(v) { return v.display_stale; });
        var signature = JSON.stringify([selected,stale,generation,vehicles.map(function(v) { return [v.owners.map(function(o) { return [o.scope,o.target.card.getAttribute('data-route-card-token')]; }),
          !!selectionFor(v),v.departure_time,v.plate_suffix,v.live_eta && v.live_eta.eta_at,v.eta_retained,v.occupancy]; })]);
        var existing = overlays[key];
        if (existing && existing.signature === signature) {
          existing.overlay.setPosition(new global.kakao.maps.LatLng(first.lat,first.lon));
          if (existing.map !== map) existing.overlay.setMap(map);
          existing.map = map; existing.group=group; updated[key] = existing; return;
        }
        var root = node('div', 'live-vehicle-marker');
        var selectable = vehicles.some(selectionFor), resumable = vehicles.some(canResumeVehicle);
        var actionable = selectable || resumable;
        var label = node(actionable ? 'button' : 'div', 'live-vehicle-label' + (selected ? ' is-selected' : '') + (stale ? ' is-stale' : '') + (actionable ? ' is-actionable' : ''));
        if (actionable) {
          label.type='button';
          label.setAttribute('aria-label',selectable ? (vehicles.length>1 ? '이동 중인 버스 '+vehicles.length+'대 선택'
            : first.departure_time+' 출발 버스 선택'+(first.plate_suffix ? ' · '+first.plate_suffix : ''))
            +(resumable ? ' · 버스 위치 확인 재개' : '') : '버스 위치 확인 재개');
          if (resumable) label.title='버스 위치 확인 재개';
          if (selectable && vehicles.length>1) { label.setAttribute('aria-haspopup','dialog'); label.setAttribute('aria-expanded',String(!!pickerState && pickerState.key===key)); }
          else if (selectable) label.setAttribute('aria-pressed',String(selected));
          bindMarkerButton(label,function() { activateGroup(key); });
        }
        var image = node('img'); image.src = '/images/app_main_icon.png'; image.alt = ''; image.width = 20; image.height = 20;
        label.appendChild(image);
        var list = node('span', 'live-vehicle-lines');
        if (vehicles.length > 1) {
          list.appendChild(node('div', 'live-vehicle-line', vehicles.length + '대 이동 중'));
        } else {
          appendVehicleLines(list,first);
        }
        label.appendChild(list);
        root.appendChild(label);
        root.addEventListener('click', function(event) { event.stopPropagation(); markerInteraction(); });
        // Informational transfer markers must also consume native map taps.
        root.addEventListener('touchend', function(event) { event.stopPropagation(); markerInteraction(); }, {passive:true});
        var overlay = existing ? existing.overlay : new global.kakao.maps.CustomOverlay({ content: root, position: new global.kakao.maps.LatLng(first.lat, first.lon), clickable: true, yAnchor: 1, zIndex: options.zIndex || 1000 });
        if (existing) { overlay.setContent(root); overlay.setPosition(new global.kakao.maps.LatLng(first.lat,first.lon)); }
        if (!existing || existing.map !== map) overlay.setMap(map);
        updated[key] = {overlay:overlay,signature:signature,map:map,root:root,group:group};
      });
      Object.keys(overlays).forEach(function(key) { if (!updated[key]) overlays[key].overlay.setMap(null); });
      overlays = updated;
      renderPicker(pickerGroup);
      if (isFinite(nextExpiry)) expiryTimer = setTimeout(renderMap, Math.max(1,nextExpiry-Date.now()));
    }
    function entryState(entry) {
      return entry.errorState || (entry.data && entry.data.state) || 'loading';
    }
    function renderRows() {
      if (!config || !config.enabled) return;
      var groups = new Map();
      entries.forEach(function(entry) {
        var card = entry.target.card;
        if (!card.isConnected || !card.classList.contains('selected')) return;
        if (!groups.has(card)) groups.set(card,[]);
        groups.get(card).push(entry);
      });
      statusViews.forEach(function(view,card) {
        if (!groups.has(card)) { hideReason(); view.row.remove(); statusViews.delete(card); }
      });
      groups.forEach(function(group,card) {
        var view = statusViews.get(card);
        if (!view || !view.row.isConnected) {
          var row = card.querySelector('.route-live-eta') || node('span','route-live-eta');
          row.setAttribute('data-position-view',group[0].viewId);
          (card.querySelector('.card-title-wrap .route-meta') || card).appendChild(row);
          view = {row:row}; statusViews.set(card,view);
        }
        card.querySelectorAll('.route-live-eta').forEach(function(row) { if (row!==view.row) row.remove(); });
        view.entries = group;
        if (idlePaused) { renderIdle(view); return; }
        // A failed leg must not hide the other legs' live positions or countdown.
        var working = group.filter(function(entry) { return !['unsupported','budget_paused','usage_unknown'].includes(entryState(entry)); });
        var state = group.every(function(entry) { return entryState(entry)==='unsupported'; }) ? 'unsupported'
          : !working.length ? 'budget_paused'
          : working.every(function(entry) { return entryState(entry)==='unavailable'; }) ? 'unavailable' : 'available';
        var issues = group.filter(function(entry) { return ['unsupported','budget_paused','usage_unknown','unavailable'].includes(entryState(entry)); });
        hideReason(); view.row.replaceChildren(); view.statusState = state;
        var separator = node('span','route-live-separator',' · '); separator.setAttribute('aria-hidden','true');
        view.row.appendChild(separator);
        view.row.appendChild(node('span','route-live-status',state==='unsupported' ? '위치 확인 불가'
          : state==='budget_paused' ? '버스 위치 갱신 일시 중단' : state==='unavailable' ? '위치 재확인 중' : '버스 위치 확인 중'));
        if (issues.length) {
          var explanations = issues.map(function(entry) {
            var reason = entry.reason || (entryState(entry)==='unavailable' ? 'service_unavailable' : entryState(entry));
            return reasonText(reason);
          });
          var sameReason = issues.length===group.length && explanations.every(function(text) { return text===explanations[0]; });
          var text = group.length===1 || sameReason ? explanations[0] : issues.map(function(entry,index) {
            return (group.indexOf(entry)+1) + '번째 셔틀' + (entry.target.context.departure_time ? ' ('+entry.target.context.departure_time+')' : '') + ': ' + explanations[index];
          }).join('\n');
          addReason(view,text);
        }
        view.counter = null;
        if (working.length) {
          var spinner = node('span','route-live-spinner'); spinner.setAttribute('aria-hidden','true');
          view.counter = node('span','route-live-countdown','0'); spinner.appendChild(view.counter); view.row.appendChild(spinner);
        }
        updateCountdown(view);
      });
    }
    function predictionKey(entry,data,vehicle) {
      return JSON.stringify([vehicle.vehicle_id,vehicle.departure_time,entry.target.context.target_stop_uuid,
        data.route_revision,data.live_service_date]);
    }
    function rememberPredictions(entry,data) {
      var prior = entry.predictions || {}, next = {};
      if (data.state!=='available' && data.state!=='empty') return;
      (data.vehicles || []).forEach(function(vehicle) {
        var key = predictionKey(entry,data,vehicle), old = prior[key];
        var age = typeof vehicle.gps_age_seconds==='number' ? vehicle.gps_age_seconds*1000 : Date.now()-new Date(vehicle.observed_at).getTime();
        if (vehicle.selected_trip && vehicle.live_eta && age>=-5000 && age<=60000
            && new Date(vehicle.live_eta.eta_at).getTime()>Date.now()) {
          // Re-reading the same GPS must not extend the prediction's lifetime.
          next[key] = old && old.observedAt===vehicle.observed_at ? Object.assign({},old,{eta:vehicle.live_eta})
            : {eta:vehicle.live_eta,observedAt:vehicle.observed_at,age:Math.max(0,age),at:entry.receivedAt};
        } else if (old && ['warming_up','off_route','ambiguous_position','previous_position_unmatched','backward_progress','profile_missing'].includes(vehicle.live_eta_state)) {
          next[key] = old;
        }
      });
      entry.predictions = next;
    }
    function render(entry, data, transientState, receivedAt) {
      if (!transientState) {
        if (entry.handoff && (['loading','unavailable'].indexOf(data.state)<0 || data.vehicles.length ||
            data.route_revision && data.route_revision!==entry.handoff.data.route_revision ||
            data.live_service_date && data.live_service_date!==entry.handoff.data.live_service_date)) entry.handoff=null;
        if (data !== entry.data) entry.receivedAt = receivedAt == null ? activityTime() : receivedAt;
        entry.data = data;
        if (data.state === 'available' || data.state === 'empty') { entry.lastGood = data; entry.lastGoodAt = entry.receivedAt; }
        rememberPredictions(entry,data);
      }
      entry.errorState = transientState || '';
      renderRows();
      renderMap();
    }
    function countdown(entry) {
      var first = !entry.firstDone && entry.firstUntil != null;
      var seconds = Math.max(0,Math.ceil((first ? entry.firstUntil-activityTime() : (entry.nextPollAt || Date.now())-Date.now())/1000));
      return {seconds:seconds,first:first};
    }
    function updateCountdown(view) {
      if (idlePaused) return;
      view.row.setAttribute('aria-busy',view.entries.some(function(entry) { return entry.pending; }) ? 'true' : 'false');
      var next = view.entries.filter(function(entry) { return !['unsupported','budget_paused','usage_unknown'].includes(entryState(entry)); })
        .map(countdown).sort(function(a,b) { return a.seconds-b.seconds; })[0] || {seconds:0};
      if (view.counter) view.counter.textContent = String(next.seconds);
      if (view.statusState!=='available' || view.row.querySelector('.route-live-info')) view.row.removeAttribute('title');
      else view.row.title = next.seconds ? (next.first ? '버스 위치 확인까지 최대 ' : '버스 위치 갱신까지 ') + next.seconds + '초' : '버스 위치 확인 중';
    }
    function updateCountdownRows() {
      statusViews.forEach(updateCountdown);
    }
    function schedulePoll(entry, token, delay) {
      entry.nextPollAt = Date.now() + delay;
      updateCountdownRows();
      entry.timer = setTimeout(function() { poll(entry, token); }, delay);
    }
    function poll(entry, token) {
      if (token !== generation || document.hidden || suspended || idlePaused || !entry.target.card.isConnected) return;
      if (idleExpired()) { pauseIdle(); return; }
      entry.pending = true;
      if (entry.firstUntil == null) entry.firstUntil = activityTime()+15000;
      entry.nextPollAt = entry.firstDone ? 0 : Date.now()+Math.max(0,entry.firstUntil-activityTime());
      updateCountdownRows();
      entry.abort = new AbortController();
      var abort = entry.abort, signal = abort.signal, timedOut = false, startedAt = activityTime();
      var requestDeadline = setTimeout(function() { timedOut = true; abort.abort(); },15000);
      var renew = !entry.watch || Date.now() >= entry.renewAt || Date.now() >= entry.watchActiveUntil;
      var watchHeaders = { 'Content-Type': 'application/json' };
      if (entry.watch) watchHeaders['X-Shuttle-Position-Capability'] = entry.watch.capability;
      var requestContext = Object.assign({}, entry.target.context, {view_id:entry.viewId});
      if (!config.gps_shared_snapshots || !requestContext.shared_route_token) delete requestContext.shared_route_token;
      var ready = renew ? fetch(options.apiBase + '/api/vehicle-positions/watch', {
        method: 'POST', credentials: 'include', signal: signal, headers: watchHeaders,
        body: JSON.stringify(requestContext)
      }).then(function(r) {
        if (r.ok) return r.json();
        return r.json().catch(function() { return {}; }).then(function(body) {
          var error = new Error(r.status === 429 || r.status >= 500 ? 'temporary' : 'unsupported');
          error.reason = body.reason || (r.status>=500 ? 'service_unavailable' : 'invalid_request'); error.retryAfter = retryAfter(r); throw error;
        });
      }).then(function(watch) {
        if (!watch.enabled) throw new Error('disabled');
        if (token !== generation) {
          fetch(options.apiBase + '/api/vehicle-positions/watch/' + encodeURIComponent(watch.watch_id) + '/close', { method: 'POST', credentials: 'include', keepalive: true, headers: { 'X-Shuttle-Position-Capability': watch.capability } }).catch(function() {});
          throw new Error('cancelled');
        }
        entry.watch = watch;
        if (entry.handoff && watch.route_revision!==entry.handoff.data.route_revision) { entry.handoff=null; renderMap(); }
        entry.renewAt = Date.now() + 100000; entry.watchActiveUntil = Date.now() + 40000; entry.etag = ''; return watch;
      }) : Promise.resolve(entry.watch);
      ready.then(function(watch) {
        if (token !== generation) return null;
        var headers = { 'X-Shuttle-Position-Capability': watch.capability };
        if (entry.etag) headers['If-None-Match'] = entry.etag;
        return fetch(options.apiBase + '/api/vehicle-positions', { credentials: 'include', signal: signal, headers: headers });
      }).then(function(r) {
        if (!r || token !== generation) return;
        if (r.status === 304) { if (!entry.data) throw new Error('temporary'); entry.watchActiveUntil = Date.now() + 40000; return entry.data; }
        if (!r.ok) { var error = new Error(r.status === 409 ? 'scope_changed' : 'temporary'); error.retryAfter = retryAfter(r); throw error; }
        return r.json().then(function(data) {
          if (data && data.enabled === false) throw new Error('disabled');
          if (!data || !Array.isArray(data.vehicles) || typeof data.state !== 'string') throw new Error('temporary');
          entry.etag = r.headers.get('ETag') || ''; entry.watchActiveUntil = Date.now() + 40000; return data;
        });
      }).then(function(data) {
        if (!data || token !== generation || document.hidden || suspended || idlePaused) return;
        entry.reason = data.reason || '';
        entry.failureCount = 0;
        var waitingFirst = !entry.firstDone && data.state==='loading' && activityTime()<entry.firstUntil;
        if (!waitingFirst) entry.firstDone = true;
        render(entry, data, undefined, startedAt);
        schedulePoll(entry, token, waitingFirst ? Math.min(1000,Math.max(1,entry.firstUntil-activityTime()))
          : Math.max(15000, Math.min(120000, Number(data.retry_after_seconds || 15) * 1000)));
      }).catch(function(error) {
        if (token !== generation || (error.name === 'AbortError' && !timedOut) || error.message === 'cancelled') return;
        if (error.message === 'scope_changed') {
          clearEntry(entry);
          entry.data = null; entry.lastGood = null; entry.handoff = null; entry.etag = '';
          entry.firstUntil = null; entry.firstDone = false; entry.reason = '';
          render(entry, {state:'loading',vehicles:[]});
          schedulePoll(entry, token, 1000);
          return;
        }
        var permanent = error.message === 'unsupported' || error.message === 'disabled';
        var state = permanent ? 'unsupported' : 'unavailable';
        entry.firstDone = true;
        entry.reason = error.reason || (error.message==='disabled' ? 'feature_disabled' : timedOut ? 'timeout' : permanent ? 'invalid_request' : 'network_error');
        if (state === 'unsupported') { entry.data = null; entry.lastGood = null; entry.handoff = null; entry.etag = ''; entry.permanentAt = lastSignal; clearEntry(entry); }
        render(entry, entry.data || {state:state,vehicles:[]}, state);
        if (!permanent) {
          entry.failureCount = (entry.failureCount || 0) + 1;
          schedulePoll(entry, token, Math.max(error.retryAfter || 0, [5000,15000,30000][Math.min(2,entry.failureCount - 1)]));
        }
      }).finally(function() {
        clearTimeout(requestDeadline);
        if (token===generation && entry.abort===abort) {
          entry.pending = false;
          updateCountdownRows();
        }
      });
    }
    function select(next, resuming) {
      var wanted = (next || []).slice(0, 3);
      if (!wanted.length) { stop(false); return; }
      var same = targets.length === wanted.length && wanted.every(function(target,index) { return contextKey(targets[index]) === contextKey(target); });
      var retryRequested = entries.some(function(entry) { return entry.permanentAt != null && lastSignal > entry.permanentAt; });
      if (entries.length && same && !retryRequested && !resuming) {
        targets = wanted;
        entries.forEach(function(entry,index) { entry.target = wanted[index]; });
        renderRows();
        renderMap(); return;
      }
      var wasPaused = idlePaused, priorActivity = lastActivity, priorWall = lastActivityWall, hadTargets = targets.length > 0;
      var handoffs = wanted.length===entries.length ? wanted.map(function(target,index) { return positionHandoff(entries[index],target,resuming); }) : [];
      stop(true,undefined,handoffs.some(Boolean));
      targets = wanted;
      if (pickerState && !wanted.some(function(target) {
        var before=pickerState.context,c=target.context;
        return target.departureSelectable && target.option===pickerState.option &&
          ['route_id','route_uuid','site_id','day_type','target_stop_uuid','service_date','historical_snapshot','shared_route_token']
            .every(function(key) { return before[key]===c[key]; });
      })) closePicker();
      idlePaused = wasPaused && (same || lastSignal <= priorActivity);
      lastActivity = !hadTargets || (wasPaused && !idlePaused) ? activityTime() : priorActivity;
      lastActivityWall = !hadTargets || (wasPaused && !idlePaused) ? Date.now() : priorWall;
      var token = generation;
      entries = targets.map(function(target,index) {
        var bytes = new Uint8Array(16); global.crypto.getRandomValues(bytes);
        var viewId = Array.from(bytes, function(b) { return b.toString(16).padStart(2,'0'); }).join('');
        return {target:target,viewId:viewId,handoff:handoffs[index] || null};
      });
      // Rebind marker owners and selection immediately, before any asynchronous watch/read.
      renderRows(); renderMap();
      configPromise.then(function(data) {
        if (token !== generation || document.hidden || suspended) return;
        if (!data.enabled) { stop(true); return; }
        if (idlePaused || idleExpired()) { idlePaused = false; pauseIdle(); return; }
        entries.forEach(function(entry) { render(entry, {state: 'loading', vehicles: []}); poll(entry, token); });
        armIdle();
        countdownTimer = setInterval(updateCountdownRows, 1000);
      });
    }
    document.addEventListener('visibilitychange', function() {
      if (document.hidden) stop(true,'visibility'); else if (targets.length) select(targets.slice());
    });
    global.addEventListener('pagehide', function() { stop(true,'pagehide'); });
    global.addEventListener('pageshow', function() { if (!document.hidden && targets.length) select(targets.slice()); });
    global.addEventListener('shuttlego:lifecycle', function(event) {
      suspended = !!(event.detail && event.detail.state === 'background');
      if (suspended) stop(true,'background'); else if (targets.length) select(targets.slice());
    });
    return { select: select, previewDeparture: previewDeparture, stop: function() { stop(false); }, ready: function() { return configPromise; },
      configReady: function() { return config !== null; },
      evidenceMode: function() { return config && config.gps_dev_experiment ? 'gps-dev-v1' : 'gps-v1'; },
      evidenceEnabled: function() { return !!(config && config.gps_evidence); } };
  }
  global.ShuttleVehiclePositions = { start: start, formatClock: clock };
})(window);
