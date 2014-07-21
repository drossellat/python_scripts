from firebase.firebase import FirebaseApplication, FirebaseAuthentication
import argparse
import sys

# Declare command-line params.
argparser = argparse.ArgumentParser(description="Firebase data extractor",add_help=False)
argparser.add_argument('-key', '--key',help='the secret key', required=True)
argparser.add_argument('-dsn', '--dsn',help='your firebase DSN', required=True)
argparser.add_argument('-email', '--email',help='your firebase email account', required=True)
argparser.add_argument('-path', '--path',help='json snapshot', required=True)

def main(argv):
   #get all the metrics passed as args
    args = argparser.parse_args()
    SECRET = args.key 
    DSN = args.dsn 
    EMAIL = args.email 
    PATH = args.path
    authentication = FirebaseAuthentication(SECRET,EMAIL, True, True)
    firebase = FirebaseApplication(DSN, authentication)
    result=firebase.get(PATH, None)
    print result

if __name__ == '__main__':
  main(sys.argv)