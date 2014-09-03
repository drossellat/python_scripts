from firebase.firebase import FirebaseApplication, FirebaseAuthentication
import argparse
import sys
import json

# Declare command-line params.
argparser = argparse.ArgumentParser(description="Firebase data extractor",add_help=False)
argparser.add_argument('-key', '--key',help='the secret key', required=True)
argparser.add_argument('-dsn', '--dsn',help='your firebase DSN', required=True)
argparser.add_argument('-email', '--email',help='your firebase email account', required=True)
argparser.add_argument('-path', '--path',help='json snapshot', required=True)
argparser.add_argument('-last', '--last',help='unix timestamp of last batch', required=True)

#get all the params passed as args
args = argparser.parse_args()
SECRET = args.key 
DSN = args.dsn 
EMAIL = args.email 
PATH = args.path
LAST = args.last

#returns new line delimited json objects for chat messages older than arg -last=unix_timestamp
def retrieveChatMessagesByTimestamp(data):
   if type(data) == dict:
      # is json a chat message or a room
      if data.has_key('room_id') or data.has_key('createdByUserName'):
        #message json only has a 'date' key/pair - room json has a 'createdAt' date key/pair
         unix_timestamp = str(data['date'])[0:-3] if data.has_key('date') else str(data['createdAt'])[0:-3]
         if float(unix_timestamp) > float(LAST):
            print json.dumps(transform(data))
      else:
         for d in data:
            if type(data[d]) == dict:
               retrieveChatMessagesByTimestamp(data[d])


#if json is a chat message,then stringify its 'data' embedded JSON, and clean up the timestamp
#if json is a room, then return total no. of authorized users in lieu of embedded JSON authorizedUsers, and clean up the timestamp
def transform(data):
  if data.has_key('date'):
    unix_timestamp = str(data['date'])[0:-3]
    data['date'] = unix_timestamp
  
  if data.has_key('data'):
    jsonString = str(json.dumps(data['data']))
    data['data'] = jsonString
  elif data.has_key('authorizedUsers'):
    unix_timestamp = str(data['createdAt'])[0:-3]
    data['createdAt'] = unix_timestamp
    #data['authorizedUsers']= data['authorizedUsers']
  return data

#main 
def main(argv):
    authentication = FirebaseAuthentication(SECRET,EMAIL, True, True)
    firebase = FirebaseApplication(DSN, authentication)
    result=firebase.get(PATH, None)
    retrieveChatMessagesByTimestamp(result)





if __name__ == '__main__':
  main(sys.argv)