# ============================================================
# simple csv file uploader for Aiseedo websocket connection
# 
# uses ai_coms for basic websocket connection and shared
# data-manipulation routines
# ============================================================

from ai_coms import *
import simplejson as json
import argparse
import csv


def main():
    parser = setupCSVLoaderParser()
    options = parser.parse_args()   
      
    try:
      if options.verbosity>=1:
        print "Connecting to " + str(options.url) + " as " + str(options.username)
      ws = csvUploaderWS(options)
      ws.connect()
      ws.run_forever()
    #except KeyboardInterrupt:
    except Exception, e:
      print "Error " + str(e)
      ws.close()


def setupCSVLoaderParser():
    parser = argparse.ArgumentParser(description="utility to upload csv file contents as JSON messages to the Aiseedo websocket interface")
    parser.add_argument("f",help="csv file to parse.  Script sends a test message if blank")

    setupArgsBasic(parser, False)
    
    parser.add_argument("-r","--rows", help="number of rows to process", type=int, default=0)
    parser.add_argument("-to","--timeoffset", help="Offset to add to the date-time, in seconds", type=float, default=0)
    parser.add_argument("-ta","--toarray", help="convert columns into a single array field", action="store_true")
    parser.add_argument("-mt","--messagetype", help="message type, defaults to the csv filename or uses _type field in the file if available")
    
    setupArgsProcessing(parser)
    
    return parser
    
class csvUploaderWS(AiseedoClient):
    'Wrapper for websocket connection, running csv file parsing at handshake completion'
    
    def __init__(self, options):
      if options.verbosity >= 2:
        print "Initialised csvUploaderWS instance"
      super(csvUploaderWS, self).__init__(options)
    
    def onconnect(self, objMsg):
      s = "Handshake complete, "
      if self.options.f:
        if self.verbosity >= 1:
          print s + "parsing and sending contents of " + str(self.options.f)
        #parse the csv file and send
        sendCsvToWS(self, self.options);
        
      else:
        if self.verbosity >= 1:
          print s + "sending test message"
        reply = {"_type":"testmessage", "data":"sent from csv uploader script"}
        self.sendObject(reply)
      
      self.close()
        


def sendCsvToWS(ws, options):
    csvfile = options.f
    head = False
    dtField = options.datefield or ""
    typefield = -1
    r=-1
    lastMsg = False
    
    if not options.live:
      #send as historic data
      m= {"_type":"Aiseedo:Command", "command": "enableUpload"}
      ws.sendObject(m)
        
    with open(csvfile, 'rb') as f:
      reader = csv.reader(f)
      for row in reader:
        r += 1
        if options.rows and r > options.rows:
          if options.verbosity >=1:
            print "Reached limit of " + str(options.rows) + " rows"
          break;
        
        if options.verbosity>=3:
          print str(row)
        elif options.verbosity >= 1 and r > 99 and  r % 100 == 0:
          print "Sent " + str(r) + " messages"
          
        if not head:
          head = row
          #locate the date field
          if dtField:
            if dtField.isdigit():
              dtField = int(dtField)
            else:
              for i, val in enumerate(head):
                if val==dtField:
                  dtField = i
                  break;
          
          if not options.messagetype:
            for i, val in enumerate(head):
              if val=="_type":
                typefield = i
                break;
            
            if typefield==-1:
              options.messagetype = csvfile.rpartition("/")[2]
              if options.messagetype[-4]==".":
                options.messagetype = options.messagetype[:-4]
          
        else:  
          lastMsg = sendOneCsvMsg(head, row, ws, options, dtField, typefield, lastMsg)
    
    
def sendOneCsvMsg(head, row, ws, options, dtField, typefield, lastMsg):
    msg = {}
    msgTime = 0
    convert = options.forcenumeric
    toarray = options.toarray
    if toarray:
      msg["data"] = []
      
    for i, val in enumerate(row):
      if convert:
        #csv files are strings!
        val = toFloat(val)

      if i != dtField :
        if toarray and i != typefield:
          msg["data"].append(val)
        else:
          msg[head[i]] = val
          
      else:
        msgTime = val
      
      if typefield == -1:
        msg["_type"] = options.messagetype
      
    msgArr = preprocessAiseedoMessage(msg, options, msgTime, lastMsg)
    for m in msgArr:
      ws.sendObject(m)  
    
    return msg
    
    
def toFloat(val):
    try:
      val = float(val)
      return val
    except:
      if not val:
        val=0
      return val


    
if __name__ == '__main__':
    main()
    



