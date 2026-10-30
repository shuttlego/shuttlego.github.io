(function(global) {
  'use strict';
  var history=[];
  function id() {
    var bytes=new Uint8Array(16);
    if(global.crypto && global.crypto.getRandomValues)global.crypto.getRandomValues(bytes);
    else for(var i=0;i<bytes.length;i++)bytes[i]=Math.floor(Math.random()*256);
    return Array.from(bytes,function(b) { return b.toString(16).padStart(2,'0'); }).join('');
  }
  function allowed(path,init) {
    var method=String(init.method || 'GET').toUpperCase();
    if(init.keepalive)return false;
    if(method==='GET' && path==='/api/vehicle-positions')return true;
    if(method!=='POST')return false;
    if(path==='/api/vehicle-positions/batch' || /^\/api\/vehicle-positions\/watch\/[^/]+\/close$/.test(path))return true;
    if(path==='/api/vehicle-positions/watch') {
      try { var body=JSON.parse(init.body);return typeof body.view_id==='string' && !!body.view_id; } catch(error) {}
    }
    return false;
  }
  function record(event) {
    history.push(event);if(history.length>100)history.shift();
    if(global.dispatchEvent && global.CustomEvent)global.dispatchEvent(new CustomEvent('shuttle:gps-network-attempt',{detail:event}));
  }
  function request(url,init) {
    init=init || {};
    var path=new URL(url,global.location.href).pathname,canRetry=allowed(path,init);
    var headers=new Headers(init.headers || {}),requestId=headers.get('X-SG-Request-Id') || id();
    headers.set('X-SG-Request-Id',requestId);
    function attempt(n) {
      var began=global.performance.now();
      function observe(extra) { record(Object.assign({request_id:requestId,attempt:n,path:path,at:Date.now(),elapsed_ms:global.performance.now()-began},extra)); }
      function retry() {
        return new Promise(function(resolve,reject) {
          setTimeout(function() {
            if(init.signal && init.signal.aborted) { reject(new DOMException('Aborted','AbortError'));return; }
            resolve(attempt(2));
          },100+Math.floor(Math.random()*100));
        });
      }
      return global.fetch(url,Object.assign({},init,{headers:headers})).then(function(response) {
        observe({status:response.status});
        if(n===1 && canRetry && [502,503,504,520,521,522,523,524,525,526].includes(response.status) && !response.headers.get('Retry-After')) {
          if(response.body && response.body.cancel)response.body.cancel().catch(function() {});
          return retry();
        }
        return response;
      },function(error) {
        observe({error_type:error.name || 'Error'});
        if(n===1 && canRetry && error.name!=='AbortError' && !(init.signal && init.signal.aborted))return retry();
        throw error;
      });
    }
    return attempt(1);
  }
  global.ShuttleGPSNetwork={fetch:request,diagnostics:function() { return history.map(function(e) { return Object.assign({},e); }); }};
})(window);
