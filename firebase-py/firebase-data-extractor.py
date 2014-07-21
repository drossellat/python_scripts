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
argparser.add_argument('-last', '--last',help='unix timestamp of last batch', required=False)

#get all the params passed as args
args = argparser.parse_args()
SECRET = args.key 
DSN = args.dsn 
EMAIL = args.email 
PATH = args.path
LAST = args.last

def retrieveLatestMessages(data):
   if type(data) == dict:
      if data.has_key('room_id') and data['type']=='plain':
         unix_timestamp = str(data['date'])[0:-3]
         if float(unix_timestamp) > float(LAST):
            print json.dumps(data)
      else:
         for d in data:
            if type(data[d]) == dict:
               retrieveLatestMessages(data[d])

def main(argv):
    authentication = FirebaseAuthentication(SECRET,EMAIL, True, True)
    firebase = FirebaseApplication(DSN, authentication)
    result=firebase.get(PATH, None)
    retrieveLatestMessages(result)





if __name__ == '__main__':
  main(sys.argv)