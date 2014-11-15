
import argparse
import sys
import json
import urllib2
import time
import datetime
import os
import logging
import ijson 
import re
import decimal
import boto
from boto.s3.connection import S3Connection

# Declare command-line params.
argparser = argparse.ArgumentParser(description="Firebase data extractor",add_help=False)
argparser.add_argument('-id', '--id',help='the access key', required=True)
argparser.add_argument('-secret', '--secret',help='the secret key', required=True)
#argparser.add_argument('-dsn', '--dsn',help='your firebase DSN', required=True)
#argparser.add_argument('-email', '--email',help='your firebase email account', required=True)
argparser.add_argument('-path', '--path',help='json snapshot', required=True)
argparser.add_argument('-last', '--last',help='unix timestamp of last batch', required=True)

#get all the params passed as args
args = argparser.parse_args()
ID = args.id
SECRET = args.secret 
PATH = args.path
LAST = args.last
FILENAME = "latest.json"
DSN = "backup.mysquar.com"

#main 
def main(argv):
    if 'messages' in PATH:
      parseMessages()
    else:
      parseRooms()

def downloadFile():
   conn = S3Connection(ID,SECRET)
   bucket = conn.lookup(DSN)

   l = [(k.last_modified, k) for k in bucket if k.name.find('.json')!=-1]
   key_to_download=sorted(l, cmp=lambda x,y: cmp(x[0], y[0]))[-1][1]
   key_to_download.get_contents_to_filename(FILENAME)

def valid_uuid(uuid):
  regex = re.compile('(pr-)*[a-f0-9]{8}-?[a-f0-9]{4}-?4[a-f0-9]{3}-?[89ab][a-f0-9]{3}-?[a-f0-9]{12}\Z', re.I)
  match = regex.match(str(uuid))
  return bool(match)

def cleanValue(val):
  if isinstance(val, decimal.Decimal) or val is None:
  return str(val)
  elif isinstance(val, bool):
   return val
  else:
  return val.encode('utf-8').strip()

def cleanDate(val):
  val = str(val)[0:-3]
  return val

def getJsonFile():
   downloadFile()
   curr_path = os.path.dirname(os.path.abspath(__file__))
   json_data=open(curr_path+'/'+FILENAME)
   f = ijson.parse(json_data)
   return f

def parseMessages():
  #f = ijson.parse(open("/Users/ADMIN/Downloads/prod-chat-messages.json", "r"))
  f=getJsonFile()
  mapKey = False
  unix_timestamp = 0
  data = False
  authorizedUsers = False
  a = {}
  for prefix, event, value in f:
    #print("prefix:%s, event:%s, value:%s" % (prefix, event, value))
    if 'chat-messages' not in prefix:
      continue
    if mapKey:
      count = prefix.count('.') 
      if count > 0 and not valid_uuid(prefix.split(".")[count]):
        key = prefix.split(".")[count]
        value=cleanValue(value)
        if data and event not in ("map_key","start_map","end_map"):
          if 'data' in a:
            a['data'][key]= value
          else:
            a['data'] = {key:value}

        if (key == 'data' or 'authorizedUsers') and event == 'start_map':
          data = True
        if key == 'data' and event == 'end_map':
          data = False
          #a['data'] = json.dumps(a['data'])

        if key == "date" or key == "createdAt":
          value = unix_timestamp = cleanDate(value)
        
        if not data and (key != 'data' and key != 'priority'):
          #print " key:%s  value:%s" %(key, value.decode('utf-8'))
          a[key] = value
        #print "key: %s  value:%s" % (key, value.decode('utf-8'))
    if (event) == ('map_key') and (valid_uuid(value)) == True:
      mapKey = True
      
    if event == ('end_map'):
      mapKey = True
      if a and float(unix_timestamp) > float(LAST):
          if 'data' in a:
            a['data'] = json.dumps(a['data'])
          print json.dumps(a)
          a= {}
          unix_timestamp = 0
          authorizedUsers = False

def parseRooms():
  f=getJsonFile()
  unix_timestamp = 0
  a = {}
  authorizedUsersDic = {}
  authorizedUsers = False
  for prefix, event, value in f:
    if 'room' not in prefix:
      continue
    count = prefix.count('.')
    key = prefix.split(".")[count] if count > 0 and not valid_uuid(prefix.split(".")[count]) else None
    uuid = True if count == 0 and  valid_uuid(prefix) else False

    if key is not None and key != 'authorizedUsers' and key !='createdAt' and event != 'map_key' and value != 'None': #entering Node?
      a[key]= cleanValue(value)
    
    if key == 'createdAt': #cleaning date format (only 10 digits)
        a[key] = value = unix_timestamp = cleanDate(value)
    
    if key == 'authorizedUsers' and event == 'map_key' and valid_uuid(value): #entering authorizedUsers node
      authorizedUsers = True
    if authorizedUsers and key is not None and key != 'authorizedUsers' and key !='createdAt' and event != 'map_key' and value != 'None': #adding authorizedUsers key/value pairs
      authorizedUsersDic[key] = cleanValue(value)
    
    if authorizedUsers and event == 'end_map': #leaving authorizedUsers node 
      if 'authorizedUsers' in a:
        a['authorizedUsers'].append(json.dumps(authorizedUsersDic))
      else:
        a['authorizedUsers'] = []
        a['authorizedUsers'].append(json.dumps(authorizedUsersDic))
      authorizedUsers = False
      authorizedUsersDic = {}

    if uuid  and event == 'end_map' and value == None: #end of node
      if a and float(unix_timestamp) > float(LAST): #skip stdout if timestamp only if timestamp is greater than LAST
           print json.dumps(a)
      a= {}
      unix_timestamp = 0


if __name__ == '__main__':
  main(sys.argv)