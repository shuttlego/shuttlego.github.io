(function(global) {
  'use strict';
  // One batch and one socket per visible controller, shared by up to three views.
  global.ShuttleGPSV2Transport = function(options) {
    var pending=[],flushTimer=null,socket=null,key='',ready=false,heartbeat=null;
    var selected=[],serial=0,failedUntil=0,pingAt=0,rtt=2000,proofs={},lastMessage=0;
    function close() {
      serial++;ready=false;key='';clearInterval(heartbeat);heartbeat=null;
      proofs={};
      if(socket) { socket.onclose=null;socket.close();socket=null; }
    }
    function flush() {
      flushTimer=null;
      var work=pending.splice(0,3).filter(function(w) {
        if(w.signal && w.signal.aborted)w.finish(new DOMException('Aborted','AbortError'));
        return !w.settled;
      });
      if(!work.length) { if(pending.length)flushTimer=setTimeout(flush,15);return; }
      var batch=new AbortController();
      work.forEach(function(w) { w.batch=batch;w.peers=work; });
      var deadline=setTimeout(function() { batch.abort(); },15000);
      fetch(options.apiBase+'/api/vehicle-positions/batch',{method:'POST',credentials:'include',
        signal:batch.signal,
        headers:{'Content-Type':'application/json','X-Shuttle-Position-Capability':work[0].watch.capability},
        body:JSON.stringify({views:work.map(function(w) { return {capability:w.watch.capability,revision:w.revision || ''}; })})
      }).then(function(response) {
        if(!response.ok) { work.forEach(function(w) { w.finish(null,response); });return; }
        return response.json().then(function(body) {
          if(!body || !Array.isArray(body.views) || body.views.length!==work.length)throw new Error('invalid batch');
          body.views.forEach(function(row,index) {
            if(row.index!==index)throw new Error('invalid batch order');
            work[index].finish(null,{ok:true,status:200,unchanged:row.unchanged,confirmation:row.data,
              headers:{get:function() { return null; }},json:function() { return Promise.resolve(row.data); }});
          });
        });
      }).catch(function(error) { work.forEach(function(w) { w.finish(error); }); })
        .finally(function() { clearTimeout(deadline); });
      if(pending.length)flushTimer=setTimeout(flush,15);
    }
    function read(watch,revision,signal) {
      return new Promise(function(resolve,reject) {
        var item={watch:watch,revision:revision,signal:signal,settled:false};
        item.finish=function(error,value) {
          if(item.settled)return;
          item.settled=true;
          if(signal)signal.removeEventListener('abort',abort);
          if(error)reject(error);else resolve(value);
        };
        function abort() {
          item.finish(new DOMException('Aborted','AbortError'));
          if(item.batch && item.peers.every(function(peer) { return peer.settled; }))item.batch.abort();
        }
        if(signal && signal.aborted) { abort();return; }
        if(signal)signal.addEventListener('abort',abort,{once:true});
        pending.push(item);
        if(!flushTimer)flushTimer=setTimeout(flush,15);
      });
    }
    function update(entries,generation) {
      var config=options.config(),next=entries.filter(function(e) { return e.watch && e.watch.stream_grant; });
      if(!config.gps_stream || !global.WebSocket || !next.length || next.length!==entries.length || Date.now()<failedUntil)return;
      var nextKey=next.map(function(e) { return e.watch.capability; }).join('|');
      if(nextKey===key)return;
      close();selected=next;key=nextKey;var token=serial,openedAt=global.performance.now();
      function failed(delay) {
        if(token!==serial)return;
        close();failedUntil=Date.now()+delay;
        if(options.onFallback)options.onFallback(next,generation);
      }
      var url=new URL(config.gps_stream_path,options.apiBase || global.location.href);
      url.protocol=url.protocol==='https:' ? 'wss:' : 'ws:';
      try { socket=new WebSocket(url.href); } catch(error) { failed(15000);return; }
      heartbeat=setInterval(function() {
        var now=global.performance.now();
        if(!ready && now-openedAt>=10000 || ready && now-lastMessage>=25000) { failed(5000);return; }
        if(ready && socket && socket.readyState===1 && now-pingAt>=15000) {
          pingAt=now;socket.send(JSON.stringify({type:'heartbeat',nonce:String(pingAt)}));
        }
      },5000);
      socket.onopen=function() {
        if(token!==serial)return;
        socket.send(JSON.stringify({type:'auth',generation:generation,views:next.map(function(e) { return {capability:e.watch.capability,grant:e.watch.stream_grant}; })}));
      };
      socket.onmessage=function(event) {
        if(token!==serial)return;
        try {
          if(typeof event.data!=='string' || event.data.length>131072)throw new Error('frame limit');
          var value=JSON.parse(event.data);
          lastMessage=global.performance.now();
          if(value.type==='ready') {
            ready=true;pingAt=lastMessage;
            return;
          }
          if(value.type==='pong') {
            if(value.nonce!==String(pingAt))return;
            rtt=Math.max(0,global.performance.now()-pingAt);
            next.forEach(function(entry) { entry.watchActiveUntil=Date.now()+40000; });return;
          }
          if(value.type==='resync') { failed(1000);return; }
          if(value.type==='proof') {
            var pendingProof=proofs[value.index];
            if(!pendingProof || pendingProof.nonce!==value.nonce || pendingProof.data.semantic_revision!==value.revision)return;
            delete proofs[value.index];
            var at=Date.parse(value.server_time),receipt=options.timeSample();
            if(!Number.isFinite(at))throw new Error('invalid proof clock');
            var verified=Object.assign({},pendingProof.data,{vehicles:pendingProof.data.vehicles.map(function(v) {
              var copy=Object.assign({},v),age=Math.max(0,at-Date.parse(v.observed_at));
              if(!Number.isFinite(age))throw new Error('invalid GPS clock');
              copy.gps_age_ms=age;copy.gps_age_seconds=Math.ceil(age/1000);
              if(copy.occupancy) { var pax=Math.max(0,at-Date.parse(copy.occupancy.received_at));
                copy.occupancy_age_ms=pax;copy.occupancy_age_seconds=Math.ceil(pax/1000); }
              return copy;
            })});
            options.onSnapshot(next[value.index],verified,generation,{mono:receipt.mono,wall:receipt.wall,epoch:receipt.epoch,
              transport:Math.max(0,receipt.mono-pendingProof.begin.mono),precise:true,uncertain:receipt.epoch!==pendingProof.begin.epoch});return;
          }
          if(value.generation!==generation || !Number.isInteger(value.index) || !next[value.index])throw new Error('scope mismatch');
          var entry=next[value.index],data=value.data;
          if(value.type==='confirm') {
            if(!entry.data || entry.data.semantic_revision!==value.revision)return;
            entry.data.last_success_at=data.last_success_at;
            if(data.source_epoch===entry.data.source_epoch)entry.data.source_sequence=Math.max(entry.data.source_sequence || 0,data.source_sequence || 0);
            if(options.onConfirmation)options.onConfirmation(entry,generation);
            return; // retain original age reference
          }
          if(value.type!=='snapshot' || !data || data.watch_id!==entry.watch.watch_id || data.route_revision!==entry.watch.route_revision ||
            data.selected_departure_time!==entry.target.context.departure_time ||
            data.live_service_date!==(entry.watch.live_service_date || entry.target.context.service_date))throw new Error('snapshot mismatch');
          var nonce=String(global.performance.now())+':'+value.index;
          proofs[value.index]={data:data,nonce:nonce,begin:options.timeSample()};
          socket.send(JSON.stringify({type:'proof',index:value.index,revision:value.revision,nonce:nonce}));
        } catch(error) { failed(30000); }
      };
      socket.onerror=function() {};
      socket.onclose=function() {
        if(token!==serial)return;
        failed(5000+Math.random()*5000);
      };
    }
    return {read:read,update:update,streaming:function(entry) { return ready && global.performance.now()-lastMessage<25000 && selected.indexOf(entry)>=0; },stop:close};
  };
})(window);
