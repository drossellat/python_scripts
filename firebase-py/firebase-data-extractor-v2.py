
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
argparser.add_argument('-local', '--local',help='to use local file', required=False)
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
LOCAL = args.local

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
  if LOCAL == None:
    downloadFile()
    curr_path = os.path.dirname(os.path.abspath(__file__))
    json_data=open(curr_path+'/'+FILENAME)
  else:
    json_data=open(LOCAL)

  f = ijson.parse(json_data)  
  return f

def parseMessages():
  f=getJsonFile()
  chatArray, dataArray = {}, {}
  afterLAST = False
  for prefix, event, value in f:
    if '.chat-messages' not in prefix:
      continue
    nodeCount = prefix.count('.')
    currentKey = value if nodeCount >= 3 and event == 'map_key' and value != '.priority' and value != None else None
    if currentKey:
      currentValue = getValue(currentKey,f)
      if currentKey == "date" or  currentKey == "createdAt":
        #print "timestamp, v, last, bool", cleanDate(currentValue), float(LAST), float(cleanDate(currentValue)) < float(LAST)
        if float(cleanDate(currentValue)) > float(LAST): 
          afterLAST = True
          currentValue = cleanDate(currentValue)
      if '.data' in prefix and cleanValue(currentValue) is not None:
        dataArray[currentKey] = cleanValue(currentValue)
      else:
        chatArray[currentKey] =  cleanValue(currentValue)
    
    if event == 'end_map' and '.data' not in prefix and chatArray and afterLAST:
      if dataArray:
        chatArray['data'] = dataArray
      print json.dumps(chatArray)
      chatArray, dataArray = {}, {}
      afterLAST = False

def getValue(currentKey, f):
  for prefix, event, value in f:
      return value

def parseRooms():
  f=getJsonFile()
  unix_timestamp = 0
  a = {}
  authorizedUsersDic = {}
  authorizedUsers = False
  for prefix, event, value in f:
    if '.rooms' not in prefix:
      continue
    count = prefix.count('.')
    key = prefix.split(".")[count] if count > 0 and not valid_uuid(prefix.split(".")[count]) else None
    uuid = True if valid_uuid(prefix.split(".")[count]) else False
    if key is not None and key != 'authorizedUsers' and key !='createdAt' and authorizedUsers is not True and event != 'map_key' and value != 'None': #entering Node?
      a[key]= cleanValue(value)

    if key == 'createdAt': #cleaning date format (only 10 digits)
      a[key] = value = unix_timestamp = cleanDate(value)

    if key == 'authorizedUsers' and event == 'map_key' and valid_uuid(value): #entering authorizedUsers node
      authorizedUsers = True
      
    if authorizedUsers and key is not None and key != 'authorizedUsers' and key !='createdAt' and event != 'map_key' and value != 'None': #adding authorizedUsers key/value pairs
      #print key, value
      authorizedUsersDic[key] = cleanValue(value)

    if authorizedUsers and event == 'end_map': #leaving authorizedUsers node
       
      if 'authorizedUsers' in a:
        a['authorizedUsers'].append(authorizedUsersDic)
      else:
        a['authorizedUsers'] = []
        a['authorizedUsers'].append(authorizedUsersDic)
      #print "authorized Users ",  a
      authorizedUsers = False
      authorizedUsersDic = {}
    if uuid  and count < 3 and event == 'end_map' and value == None: #end of node
      #print "k,p,c,e,v,u,ts", key, prefix, count, event, value, uuid, unix_timestamp
      if a and float(unix_timestamp) > float(LAST): #skip stdout if timestamp only if timestamp is greater than LAST
        print json.dumps(a)
      a= {}
      unix_timestamp = 0


if __name__ == '__main__':
  main(sys.argv)
