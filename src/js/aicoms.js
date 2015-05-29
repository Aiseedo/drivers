//See https://aiseedo.com/doc/resources/aicomsjs/ for documentation

function AiComs(url, feedKey, feedPassword, projectKey, projectPassword, instanceName) {
  this.url = url
  this.feedKey = feedKey;
  this.feedPassword = feedPassword;
  this.projectKey = projectKey;
  this.projectPassword = projectPassword;
  this.state = "unknown";
  this.instanceName = instanceName || "Unnamed";
  this.reportedNotReady = false;
  this.active = true;
  this.incomingCount=0
  this.outgoingCount=0
  
  // debug and messaging
  this.raiseDebug = true;
  this.debuglog("Asked to connect to feed " + feedKey);
  this.debuglogToConsole = false;
  this.logAllMessages = false;
  
  //reconnection and pinging
  this.cacheUnsent = true; //store messages until connected
  this.unsent=[]
  this.disconnectCalled = false;
  this.reconnectAttempts = 100; //Set to zero to switch off reconnection
  this.reconnectInterval = 2000; //milliseconds
  this.reconnectDecay = 1.5 //back-off the reconnect attempts
  this.reconnectIfHidden = false //only if visible window, by default
  this.hiddenProp = "unchecked"
  
  this.pingInterval = 0 //milliseconds.  Set to zero to switch off pinging
  this.pingCounter = 0;
  this.pingTimer = 0
  this.reconnectTimer=0
  this.setReconnect(this.reconnectInterval, this.reconnectAttempts, this.reconnectDecay)
  this.setPing(this.pingInterval)
  
  this.connect();
}

AiComs.prototype.connect = function() {
  if (!this.active) { return; }
  this.disconnectCalled = false;
  //no point connecting until we have a password/key combination
  //as we won't be able to respond to the connect-challenge
  if (!(this.feedPassword && this.feedKey) && !(this.projectKey && this.projectPassword)) { return; }
  
  if (this.ws){
    //close and delete
    this.ws.onclose = function(){}
    this.ws.onerror = function(){}
    this.ws.close()
    this.ws = null
  }
  
  if ("WebSocket" in window) {
    this.ws = new WebSocket(this.url);
  } else if ("MozWebSocket" in window) {
    this.ws = new MozWebSocket(this.url);
  } else {
    this.debuglog("Error connecting to: " + this.url + ", no websocket capability")
    this.state = "error";
    this.onstatechange()
  }
  
  var obj = this;
  
  this.ws.onopen = function(e) {
    obj.debuglog("Connection opened for " +  obj.feedKey);
    obj.reportedNotReady = false;
    obj.reconnectTimer.stop()
  };
  
  this.ws.onclose = function(e) {
    obj.debuglog("Connection closed for " + obj.feedKey);
    obj.state = "disconnected"
    obj.onstatechange()
    //set a reconnect attempt if this wasn't intentional
    if (!obj.disconnectCalled) { obj.reconnectTimer.start(); }
    obj.ondisconnect(e);
  };

  this.ws.onerror = function(e) {
    obj.debuglog("Connection error for " + obj.feedKey + " on " + obj.url);
    obj.onerror(e);
  };

  this.ws.onmessage = function(e) {
    obj.handlemsg(e);
  };
}
/* events raised to calling code */
AiComs.prototype.onstatechange = function(){
}

AiComs.prototype.onconnect = function() {
}

AiComs.prototype.ondisconnect = function() {
}

AiComs.prototype.onerror = function() {
}

AiComs.prototype.onmessage = function() {
}

AiComs.prototype.onsend = function(msg) {
}

AiComs.prototype.onnewfeed = function(feedId,feedPassword) {
}
/* end of callback events */

AiComs.prototype.disconnect = function() {
  this.reconnectTimer.stop()
  this.disconnectCalled = true;
  
  if (this.ws) { this.ws.close(); }
  this.state = "disconnected"
  this.onstatechange()
}

AiComs.prototype.send = function(reply, authenticationMsg) {

  if (this.ws === undefined || this.ws.readyState != 1) {
    this.reportedNotReady = true;
    if (!authenticationMsg) {        
      if (this.cacheUnsent){
        if (this.logAllMessages){ this.messagelog("Cacheing: " + JSON.stringify(reply));}
        this.unsent.push(reply)          
        
      } else {
        this.debuglog("Websocket not ready for writing but send command called");
      }
    }
    return;
  } else if (this.state != "connected" && !authenticationMsg){
     if (this.cacheUnsent){ 
       if (this.logAllMessages){ this.messagelog("Cacheing: " + JSON.stringify(reply));}
       this.unsent.push(reply)       
       
     } else {
       this.debuglog("Coms object not connected but send command called");
     }
     return
  }

  var replyStr = JSON.stringify(reply);
  if (this.logAllMessages){ this.messagelog("Sending: " + replyStr);}
  this.ws.send(replyStr);
  this.onsend(reply)
  this.outgoingCount++
}

AiComs.prototype.subscribe = function(topics) {
  var subscribeMsg = {
      '_type' : 'Aiseedo:Subscribe',
      'operation' : 'add',
      'topics' : topics
    };
  
  this.send(subscribeMsg);
}

AiComs.prototype.clearCache = function(){
  this.unsent = []
}

AiComs.prototype.handlemsg = function(e) {
  this.incomingCount++
  if (!this.active) { return; }
  try{
  var obj = JSON && JSON.parse(e.data) || $.parseJSON(e.data);
  if (this.logAllMessages) {
    this.messagelog("Got message " + e.data + " in feed " + this.feedKey)
  }
    
  if (obj._type == "Aiseedo:ConnectChallenge") {
    if (!this.feedKey) {
      if (this.projectKey) {
        this.debuglog("Creating instance for project " + this.projectKey);
        var reply = {
          _type:'Aiseedo:CreateInstance',
          appKey:this.projectKey, 
          authenticationMethod: "MD5", 
          password: hex_md5(obj.challenge + this.projectPassword),
          instanceName:this.instanceName,
          accessType:"Feed"
        };
        this.send(reply, true);
      } else {
        this.debuglog("No project given to connect to...");
      }
    } else {
      this.debuglog("Connecting to feed " + this.feedKey);
      this.challenge = obj.challenge;
      reply = {
        '_type' : 'Aiseedo:FeedAccess',
        'feedAccessId' : this.feedKey,
        'authenticationMethod' : 'MD5',
        'password' : hex_md5(obj.challenge + this.feedPassword),
        'accessType' : 'Feed'
      };
      this.send(reply, true);
    }
    return;
  }
  if(obj._type=="Aiseedo:FeedAccess") {
    this.onnewfeed(obj.feedAccessId,obj.password);
    return ;
  }
  if (obj._type == "Aiseedo:Command") {
    if (obj.command == "Ready") {
      this.state = "connected"
      this.onstatechange()
      if (this.pingInterval) { this.pingTimer.start() }
      this.replayCache();
      this.onconnect();
      return ;
    }
  }
  if (obj._type == "Aiseedo:ErrorC") {
    if(obj.message=="Feed not known.") {
      if(this.projectKey) {
        // Try and create a new instance.
        var reply = {
               _type:'Aiseedo:CreateInstance',
               appKey:this.projectKey,
               authenticationMethod: "MD5", 
               password: hex_md5(obj.challenge + this.projectPassword),
               instanceName:this.instanceName,
               accessType:"Feed"
               };
      }
    
      this.send(reply,true);
      return ;
    }
    this.onerror(obj);
    return;
  }

  this.onmessage(obj);
  }
  catch(err){
   this.debuglog("Error handling returned message: " + e.data)
  }
}

AiComs.prototype.reset = function(){
  if (this.state=="connected"){ this.disconnect() }
  this.active = false
  this.clearCache()
  if (this.ws){
    this.ws.onopen = function(){ return; }
    this.ws.onclose = function(){ return; }
    this.ws = null;      
  }
}

AiComs.prototype.replayCache = function(){
  if (!this.cacheUnsent || this.unsent.length==0 ) { return;}
  for (var i=0,u; u=this.unsent[i];i++){
    this.send(u);
  }
  this.clearCache();
  this.debuglog("Sent " + i + " cached messages")
}

AiComs.prototype.onDebugMessage = function(){
}

AiComs.prototype.messagelog = function(message){
  if (this.debuglogToConsole) { console.log(message); }
  if (this.raiseDebug) { this.onDebugMessage(message,true) }
}

AiComs.prototype.debuglog = function(message){
  if (this.debuglogToConsole) { console.log(message); }
  if (this.raiseDebug) { this.onDebugMessage(message) }
}

AiComs.prototype.setReconnect = function(interval, attempts, decay){
  this.reconnectAttempts = attempts
  this.reconnectInterval = interval
  this.reconnectDecay = decay || this.reconnectDecay
  this.reconnectTimer = aiGetComsTimer(interval, attempts, this.reconnectDecay, "reconnect", this)
}

AiComs.prototype.setPing = function(interval){
  this.pingInterval = interval
  this.pingTimer = aiGetComsTimer(interval, 0, 1, "ping", this)
}

AiComs.prototype.documentHidden = function(){
  if (!this.hiddenProp) { return false; }

  if (this.hiddenProp == "unchecked"){
    var prefixes = ['h','webkitH','mozH','msH','oH']
    for (var i=0, p; p=prefixes[i]; i++){
      if (typeof document[p + "idden"] !== "undefined"){
        this.hiddenProp = p + "idden"
        break;
      }
    }
    if (!this.hiddenProp){ 
      this.hiddenProp = false
      return false;
    }
  }
  
  //browser supports hidden
  return document[this.hiddenProp]
}

function aiGetComsTimer(interval, attempts, decay, type, coms) {
  var currentNum, lag, timerRef;

  function start() {
    if (timerRef) { return; }
    currentNum = attempts;
    lag = interval/decay;
    timerRef = setTimeout(run, lag);
  }

  function run() {
    if (timerRef) stop();
    //either reconnect, or ping
    if (type == "reconnect"){
      if (!attempts) { return; }
      if (coms.reconnectIfHidden || !coms.documentHidden() ){
        coms.connect()
        if (currentNum--) {
          //back off the lag, also add noise so all clients aren't reconnecting at the same time
          lag = lag * decay * (0.9 + 0.2*Math.random())
        } else {
          coms.debuglog("Reached reconnect limit for " + coms.feedKey)
          return;
        }
      } else {
        //console.log("hidden, not connecting")
      }
      timerRef = setTimeout(run, lag);
      
    } else if (type=="ping") {
      //ping - TODO
             
      //timerRef = setTimeout(run, lag);
    }
    
  }

  function stop() {   
    if (timerRef) {
     clearTimeout(timerRef);
     timerRef = 0
    }
  }

  return {
    start: start,
    run: run, 
    stop: stop
  };
}

//========== Utilities ==========================

window.displayComsStatus = function(el,status){
  if (Array.isArray(el)){
    for (var i=0, e; e=el[i]; i++){
      displayComsStatus(e,status) 
    }
    return;
  }
  if (typeof el === "string"){ el = document.getElementById(el) }
  if (status=="connected"){
    el.innerHTML = "<i class='fa fa-check-circle'></i>  Connected"
    el.style.color = "green"
  } else if (status=="disconnected"){
    el.innerHTML = "<i class='fa fa-exclamation'></i>  Disconnected"
    el.style.color = "red"    
  } else {
    status = status || "unknown"
    el.innerHTML = "<i class='fa fa-circle-thin'></i>  " + status
    el.style.color = ""
  }
}

//==========  resource-load check  ==============
window.aijsComsScript = function(){
  //check md5 dependency
  return !!hex_md5
}

