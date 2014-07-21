import requests
payload={'where':'{"createdAt":{"$lte":{"__type":"Date","iso":"2014-07-14T00:00:00.000Z"}}}','count':'1', 'limit':'0'}
headers = {'X-Parse-Application-Id': 'xxxxxxxxxxxxxxxxxxxxxx', 'X-Parse-REST-API-Key':'xxxxxxxxxxxxxxxxxxxxx'}
r=requests.get('https://api.parse.com/1/classes/Score', params=payload, headers=headers)