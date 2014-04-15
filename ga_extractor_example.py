#!/usr/bin/python
# -*- coding: utf-8 -*-
#using this as base https://code.google.com/p/google-api-python-client/source/browse/samples/analytics/core_reporting_v3_reference.py



import argparse
import sys
import json

from apiclient.errors import HttpError
from apiclient import sample_tools
from oauth2client.client import AccessTokenRefreshError


# Declare command-line flags.
argparser = argparse.ArgumentParser(description="GA data extractor",add_help=False)
argparser.add_argument('-t', '--table_id',help='The table ID of the profile you wish to access. Format is ga:xxx where xxx is your profile ID.', required=True)
argparser.add_argument('-s','--start_date', help='the start date', required=True)
argparser.add_argument('-e','--end_date', help='the end date', required=True)
argparser.add_argument('-m','--metrics', help='metrics format is ga:xxxx', required=True)
argparser.add_argument('-d','--dimensions', help='metrics format is ga:xxxx', required=False)


def main(argv):
  # Authenticate and construct service.

  service, flags = sample_tools.init(
      argv, 'analytics', 'v3', __doc__, __file__, parents=[argparser],
      scope='https://www.googleapis.com/auth/analytics.readonly')
 # Try to make a request to the API. Print the results or handle errors.
  try:
    results = get_api_query(service, flags).execute()
    print json.dumps(results)

  except TypeError, error:
    # Handle errors in constructing a query.
    print ('There was an error in constructing your query : %s' % error)

  except HttpError, error:
    # Handle API errors.
    print ('Arg, there was an API error : %s : %s' %
           (error.resp.status, error._get_reason()))

  except AccessTokenRefreshError:
    # Handle Auth errors.
    print ('The credentials have been revoked or expired, please re-run '
           'the application to re-authorize')

# def set_api_query(**kargs):
#   #test

def get_api_query(service,flags):
  """Returns a query object to retrieve data from the Core Reporting API.

  Args:
    service: The service object built by the Google API Python client library.
    table_id: str The table ID form which to retrieve data.
  """

  return service.data().ga().get(
      ids=flags.table_id,
      start_date=flags.start_date,
      end_date=flags.end_date,
      metrics=flags.metrics,
      dimensions=flags.dimensions,
      #sort='-ga:visits',
      #filters='ga:medium==organic',
      start_index='1',
      max_results='25')



if __name__ == '__main__':
  main(sys.argv)