# ============================================================
# Connection to Aiseedo service via websockets
# and useful data-manipulation functions
# 
# ============================================================

from ws4py.client.threadedclient import WebSocketClient
import simplejson as json
import hashlib
import argparse
from datetime import datetime


#set up arguments for wrapper scripts
def setupArgsBasic(parser, required):
    if required:
      parser.add_argument("username", help="username (feed key or project access key)" )
      parser.add_argument("password", help="password")      
    else:
      parser.add_argument("-u","--username", help="username (feed key or project access key)" )
      parser.add_argument("-p","--password", help="password")
    parser.add_argument("-ur","--url", help="websocket url, defaults to wss://aiseedo.com:8022/feed", default="wss://aiseedo.com:8022/feed")
    parser.add_argument("-v","--verbosity", action="count", default=0, help="increase output verbosity, set as -v, -vv etc.")

def setupArgsProcessing(parser):
    parser.add_argument("-d","--datefield", help="the field to use as the input date")
    parser.add_argument("-l","--live", help="load the messages as live, i.e. stamped with the time of receipt at the server", action="store_true")
    parser.add_argument("-df","--dateformat", help="format of input date fields")
    parser.add_argument("-ad","--adddayofweek", help="add the day of the week to messages", action="store_true")
    parser.add_argument("-fn","--forcenumeric", help="convert numeric values from strings to numbers", action="store_true", default="true")
    parser.add_argument("-am","--addmessage", help="add additional time data as a separate message instead of additional fields", action="store_true")
    parser.add_argument("-del","--deltafields", help="add delta fields for numeric fields, comma-separate the original and new fieldnames, semicolon-separate the fields")

# ==================================================
# 
# Connectivity to Aiseedo service
#
# ==================================================

    
class AiseedoClient(WebSocketClient):
    #expects options to contain url, key and password
    
    def __init__(self, options):
      self.options = options
      if options.url=="local":
        options.url="ws://127.0.0.1:8021/feed"
      super(AiseedoClient, self).__init__(options.url, protocols=['http-only', 'chat'])
      
    def opened(self):
      o = self.options
      self.feed = o.username
      self.password = o.password
      self.verbosity = o.verbosity
      if self.verbosity >= 1:
        print "Opened websocket at " + str(o.url) + " as " + str(o.username)

    def closed(self, code, reason=None):
      if self.verbosity >= 1:
        print "Closed websocket", code, reason

    def sendObject(self, msgObj):
      data = json.dumps(msgObj)
      self.send(data)
      if self.verbosity >= 2:
        print "Sending message %s " % (data)
      
      
    def received_message(self, m):
      if self.verbosity >= 2:
        print "Received message %s " % (m)
        
      obj = json.loads(m.data)
      typeName = obj["_type"]
      if typeName == 'Aiseedo:ConnectChallenge':
        if self.verbosity >= 2:
          print "Got connect message in state "
        challenge = obj["challenge"]
        m = hashlib.md5()
        m.update(challenge)
        m.update(self.password)
        chapReply = m.hexdigest()
        reply = {'_type':'Aiseedo:FeedAccess',
                  'feedAccessId':self.feed, 
                  'authenticationMethod': 'MD5', 
                  'password': chapReply,
                  'accessType':'Feed'
                  }
        self.sendObject(reply)
        
      if typeName == 'Aiseedo:Command':
        if obj['command'] == "Ready":
          self.onconnect(obj)
          
        else:  
          self.onmessage(obj)
    
    #callback functions
    
    def onmessage(self, objMsg):
      if self.verbosity >= 3:
        print "Received message " + json.dumps(objMsg)

    def onconnect(self, objMsg):
      if self.verbosity >= 1:
        s = "Aiseedo handshake complete"


# ==================================================
# 
# Directly-called operation
#
# ==================================================

def main():
    #send a message over the websocket and exit
    parser = argparse.ArgumentParser(description="Python module to facilitate access to the Aiseedo websocket interface")
    setupArgsBasic(parser, True)
    parser.add_argument("-m","--message", help="message to send. Sends a test message if blank")
    parser.add_argument("-c","--count", help="number of times to send the message. Defaults to 1", type=int, default=1)
    
    options = parser.parse_args()   
    try:
      if options.verbosity>=1:
        print "Connecting to " + str(options.url) + " as " + str(options.username)
      ws = simpleUploaderWS(options)
      ws.connect()
      ws.run_forever()
    #except KeyboardInterrupt:
    except Exception, e:
      print "Error " + str(e)
      ws.close()
  

class simpleUploaderWS(AiseedoClient):
    'Wrapper for websocket connection, sending file at handshake completion'
    
    def __init__(self, options):
      super(simpleUploaderWS, self).__init__(options)
    
    def onconnect(self, objMsg):
      s = "Handshake complete, "
      msg = self.options.message
      if msg:
        msg = json.loads(msg)
      else:
        if self.verbosity >= 1:
          print s + "sending test message"
        msg = {"_type":"testmessage", "data":"sent from ai_coms.py module"}        
      
      for x in range(0, self.options.count):
        self.sendObject(msg)
      
      self.close()

        
if __name__ == '__main__':
    main()
      
    
# ==================================================
# 
# Data processing functions
#
# ==================================================

CONST_DAYS = ["MON","TUE","WED","THU","FRI","SAT","SUN"]

def preprocessAiseedoMessage(msg, options, msgTime, lastMsg):
    #enrich the message before sending to the Aiseedo service
    global CONST_DAYS
    msg2 = {"_type":"_ai_dateinfo"}
    isnewmsg = False
    
    if options.datefield and msgTime:
      if len(msgTime)==10:
        #date only
        msgTime += " 00:00:00.000001"
      oTime = parseTime(msgTime, options);    
    else:
      oTime = datetime.now()
    
    if options.adddayofweek:
      if options.addmessage:
        msg2["_day"] = CONST_DAYS[oTime.weekday()]
        isnewmsg = True
      else:
        msg["_day"] = CONST_DAYS[oTime.weekday()]
    
    if options.deltafields and lastMsg:
      dfs = options.deltafields.split(";")
      for df in dfs:
        if "," not in df:
          df = df + "," + df + "_delta"
        df = df.split(",")
        if msg[df[0]]:
          try:
            msg[df[1]] = float(msg[df[0]]) - float(lastMsg[df[0]])
          except Exception, e:
            if options.verbosity>=3:
              print "Error calculating delta field: " + str(e)
            msg[df[1]] = "error"
    
    if not options.live:
      #wrap for historic data upload
      msgTime = timeString(oTime, msgTime, options.verbosity)
      msg = {"_type" : "Aiseedo:Message", "time" : msgTime, "content" : msg}
      if isnewmsg:
        msg2 = {"_type" : "Aiseedo:Message", "time" : msgTime, "content" : msg2}
    
    msg = [msg]
    if isnewmsg:
      msg.append(msg2)
    
    return msg
    
    
def parseTime(msgTime, options):
    mask = "%Y-%m-%d %H:%M:%S.%f"
    
    if options.dateformat:
      mask = options.dateformat
    
    return datetime.strptime(msgTime, mask)
  
  
def timeString(oTime, msgTime, verbosity):
    try:
      return oTime.strftime("%Y-%m-%d %H:%M:%S.%f")
    except Exception, e:
      if verbosity >=2:
        print "Error converting datetime object to string " + str(e)
      return msgTime
    
    
def isFloat(val):
    try:
      v = float(val)
      return true
    except:
      return false
    