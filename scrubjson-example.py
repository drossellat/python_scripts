import json
import urllib2
import time
import subprocess
import datetime
import os
import logging



def retrieveLastTimeStamp():
	try:
		myvar = open('timestamps.log', 'r')
		lst = myvar.readlines()
	except:
		return 0
	else:
		return lst[len(lst)-1]

def saveTimeStamp(): #throws a IOError if no file
	fileName = "save_%d.json" % currentTimeStamp
	with open(fileName):
		f= open( "timestamps.log" , "a" )
		f.write(str(currentTimeStamp)+"\n")
		f.close()

def queryFirebaseJson(fURL):
	firebaseData=urllib2.urlopen(fURL)
	js = json.load(firebaseData)
	return js

def saveToFile(data):
	if float(data['date_chat']) < float(previousTimeStamp): #no need to commit json to file
		return
	f= open( "save_%d.json" % currentTimeStamp , "a" )
	data = json.dumps(data)+'\n'
	f.write(data)
	f.close()

def generateBigQueryJsonList(data, jsonObj={}, level=0):
	if type( data ) == dict:

		for d in data:
			if level == 0:
				jsonObj['roomId']= d
				generateBigQueryJsonList(data[d], jsonObj, 1)
			elif level == 1:
				jsonObj['chatId']= d
				generateBigQueryJsonList(data[d],jsonObj, 2)
			elif level == 2:
				if d =='timestamp':
					data[d] = int(str(data[d])[0:-3])
					jsonObj['date_chat']= data[d] #renaming 'timestamp' to 'date_chat' as Google Big Query doesnt like fields named 'timestamp'
				else:
					jsonObj[d]= data[d]				
				generateBigQueryJsonList(data[d], jsonObj, 2)		
		if level==2: # save json to file 
			saveToFile(jsonObj)

def copyToGoogleStorage():
	fileName = "save_%d.json" % currentTimeStamp
	cmd = "gsutil cp %s gs://chat-data/" % fileName	
	with open(fileName): #only copy to google storage if json file exists
		subprocess.call(cmd, shell=True)

def dumpToBigQuery():
	fileName = "save_%d.json" % currentTimeStamp
	cmd = "bq load --source_format=NEWLINE_DELIMITED_JSON chats.chats_table gs://chat-data/%s name:string,date_chat:timestamp,userId:string,userAvatar:string,type:string,message:string,roomId:string,chatId:string" % fileName	
	with open(fileName): #only dump data if json file exists
		subprocess.call(cmd, shell=True)



#replace auth token value with real value from firebase
firebaseURL = "https://squar.firebaseio.com/chat/room-messages.json?auth=XXXXXXXXXXXXXXX"
logging.basicConfig(level=logging.DEBUG, filename='scrubjson.log')
currentTimeStamp = time.time()
previousTimeStamp = retrieveLastTimeStamp()

def main():
	try:
		generateBigQueryJsonList(queryFirebaseJson(firebaseURL))
		saveTimeStamp()
		logging.info("firebase query successful for timestamp: %d" % currentTimeStamp)
	except:
		logging.exception("process failed for timestamp: %d" % currentTimeStamp)
	try:
		copyToGoogleStorage()
		logging.info("Google Storage copy successful for timestamp: %d" % currentTimeStamp)
	except:
		logging.exception("Google Storage copy failed for timestamp: %d" % currentTimeStamp)
	try:
		dumpToBigQuery()
		logging.info("Google Big Query dump successful for timestamp: %d" % currentTimeStamp)
	except:
		logging.exception("Google Big Query dump failed for timestamp: %d" % currentTimeStamp)
main()




