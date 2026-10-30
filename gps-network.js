(function(global) {
  'use strict';
  var history=[];
  var TOTAL_MS=6000,FIRST_MS=2000;
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
    var deadline=global.performance.now()+TOTAL_MS;
    function timeoutError() { return new DOMException('GPS request timed out','TimeoutError'); }
    function abortError() { return new DOMException('Aborted','AbortError'); }
    function discard(response) {
      if(response && response.body && response.body.cancel)response.body.cancel().catch(function() {});
    }
    function attempt(n) {
      if(init.signal && init.signal.aborted)return Promise.reject(abortError());
      var began=global.performance.now(),controller=canRetry && global.AbortController ? new global.AbortController() : null;
      var budget=Math.max(0,Math.min(n===1?FIRST_MS:TOTAL_MS,deadline-began));
      var timer=null,response=null,timedOut=false,cancelled=false,rejectDeadline;
      function observe(extra) { record(Object.assign({request_id:requestId,attempt:n,path:path,at:Date.now(),elapsed_ms:global.performance.now()-began,
        deadline_supported:!!controller,attempt_budget_ms:controller?budget:null,total_budget_ms:controller?TOTAL_MS:null},extra)); }
      function onAbort() { cancelled=true;controller.abort();rejectDeadline(abortError()); }
      function cleanup() {
        if(timer!==null)clearTimeout(timer);
        if(controller && init.signal && init.signal.removeEventListener)init.signal.removeEventListener('abort',onAbort);
      }
      function retry() {
        var delay=100+Math.floor(Math.random()*100);
        if(controller && deadline-global.performance.now()<=delay)return Promise.reject(timeoutError());
        return new Promise(function(resolve,reject) {
          setTimeout(function() {
            if(init.signal && init.signal.aborted) { reject(abortError());return; }
            resolve(attempt(2));
          },delay);
        });
      }
      var options=Object.assign({},init,{headers:headers});
      if(controller)options.signal=controller.signal;
      var work=Promise.resolve().then(function() { return global.fetch(url,options); }).then(function(value) {
        response=value;
        if(timedOut || cancelled) { discard(response);throw timedOut?timeoutError():abortError(); }
        var gateway=n===1 && canRetry && [502,503,504,520,521,522,523,524,525,526].includes(response.status) && !response.headers.get('Retry-After');
        // Wait for the JSON body too: headers alone must not end the deadline.
        // The original response stays unread and retains URL/status/header metadata.
        var read=controller && !gateway && response.body && response.clone ? response.clone().arrayBuffer() : Promise.resolve();
        return Promise.resolve(read).then(function() { return {response:response,retry:gateway}; });
      });
      if(controller) {
        var expiration=new Promise(function(resolve,reject) {
          rejectDeadline=reject;
          timer=setTimeout(function() { timedOut=true;controller.abort();reject(timeoutError()); },budget);
          if(init.signal && init.signal.addEventListener)init.signal.addEventListener('abort',onAbort,{once:true});
          if(init.signal && init.signal.aborted)onAbort();
        });
        work=Promise.race([work,expiration]);
      }
      return work.then(function(value) {
        cleanup();observe({status:value.response.status,phase:'complete'});
        if(value.retry) {
          discard(value.response);
          return retry();
        }
        return value.response;
      },function(error) {
        cleanup();discard(response);
        if(init.signal && init.signal.aborted)error=abortError();
        else if(timedOut)error=timeoutError();
        observe({error_type:error.name || 'Error',status:response?response.status:null,phase:response?'body':'headers',timed_out:timedOut});
        var retryableResponse=!response || (response.status<400 && !response.headers.get('Retry-After'));
        if(n===1 && canRetry && retryableResponse && !(init.signal && init.signal.aborted) && (timedOut || error.name!=='AbortError'))return retry();
        throw error;
      });
    }
    return attempt(1);
  }
  global.ShuttleGPSNetwork={fetch:request,diagnostics:function() { return history.map(function(e) { return Object.assign({},e); }); }};
})(window);
