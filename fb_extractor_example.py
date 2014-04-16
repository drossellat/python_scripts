#!/usr/bin/python
# coding: utf-8

import facebook
import urllib
import urlparse
import subprocess
import warnings
import json
import argparse
import sys
import os

# Hide deprecation warnings. The facebook module isn't that up-to-date (facebook.GraphAPIError).
warnings.filterwarnings('ignore', category=DeprecationWarning)

# Declare command-line params.
argparser = argparse.ArgumentParser(description="FB data extractor",add_help=False)
argparser.add_argument('-m', '--metrics',help='metrics you want to fetch, comma separated', required=True)

def main(argv):
  
  #retrieve ultra-secret credentials fb_secrets.json 
  curr_path = os.path.dirname(os.path.abspath(__file__))
  json_data=open(curr_path+'/fb_secrets.json')
  data = json.load(json_data)
  facebook_app_id= data['app_id']
  facebook_app_secret = data['app_secret']
  facebook_profile_id = data['profile_id']

  #get all the metrics passed as args
  args = argparser.parse_args()
  metrics = [x.strip() for x in args.metrics.split(',')]

  # Trying to get an access token. Very awkward.
  oauth_args = dict(client_id     = facebook_app_id,
                    client_secret = facebook_app_secret,
                    grant_type    = 'client_credentials')
  oauth_curl_cmd = ['curl',
                    'https://graph.facebook.com/oauth/access_token?' + urllib.urlencode(oauth_args)]
  oauth_response = subprocess.Popen(oauth_curl_cmd,
                                    stdout = subprocess.PIPE,
                                    stderr = subprocess.PIPE).communicate()[0]

  try:
      oauth_access_token = urlparse.parse_qs(str(oauth_response))['access_token'][0]
  except KeyError:
      print('Unable to grab an access token!')
      exit()

  facebook_graph = facebook.GraphAPI(oauth_access_token)

  #retrieve insights
  json_data = {}
  for metric in metrics:
    try:
        fb_response = facebook_graph.get_object('141217169407310/insights/%s' % metric)
        json_data[metric]=fb_response['data']
        
    except facebook.GraphAPIError as e:
        print 'Something went wrong:', e.type, e.message
  print json.dumps(json_data)


if __name__ == '__main__':
  main(sys.argv)