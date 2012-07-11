#!/usr/bin/python
"""

Script that uploads stored logs to amazon s3

"""

import os
import sys
from datetime import date,timedelta

sys.path.append('<folder>')

import settings

from boto.s3.connection import S3Connection
from boto.s3.key import Key


def uploadFile(root,day,name,bucket):
	s3object = Key(bucket)
	s3object.key = "/".join([day,name])
	s3object.set_metadata("Content-Type", 'Text/plain')
	try:
		s3object.set_contents_from_filename("/".join([root,day,name]))
	except:
		return 1
	return 0
			
def deleteFile(root,day,name):
	try:
		os.remove("/".join([root,day,name]))
	except:
		return 1

def uploadDir(root,day):
	bucketName = ".".join(["<redacted>",str(settings.NODE_ID),"logs"])
	
	s3conn = S3Connection(settings.AMAZON_S3_ACCESS_KEY,settings.AMAZON_S3_SECRET_KEY)
	bucket = s3conn.get_bucket(bucketName)
	
	try:
		logs = os.listdir("/".join([root,day]))
	except:
		return 1
	for log in logs:
		if uploadFile(settings.LOGS_FOLDER,day,log,bucket) is 0:
			deleteFile(settings.LOGS_FOLDER,day,log)
		
def upload(day):
	uploadDir(settings.LOGS_FOLDER,day)
		
def uploadAll():
	days = os.listdir(settings.LOGS_FOLDER)
	
	today = date.today()
	
	for day in days:
		if day != today.strftime("%Y%m%d"):
			uploadDir(settings.LOGS_FOLDER,day)
	

def main():
	yesterday = date.today() - timedelta(1)
	upload(yesterday.strftime("%Y%m%d"))

	#uploadAll()
	

if __name__ == '__main__':
	main()