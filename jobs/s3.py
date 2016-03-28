#!/usr/bin/env python
import pickle
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3 import connect_to_region

from s3_key import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

reg = 'us-west-2'

def upload_to_s3( target_path, data_string ):
	""" 
	Uploads a string to S3, located in the sg14fbr bucket at the given path. 
	All of the data is put inside the 'data' folder on S3.  For example:
	target_path='twitter/06-25-14-300.json', data_string='{"some":"big","json":"object"}'
	This would place the json string into a file on S3 located at data/twitter/06-25-14-300.json
	"""
	conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
	#conn = connect_to_region(reg, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
	bucket = conn.get_bucket('cddl-riyadh-2')
	# for key in bucket.list():
	# 	print key.name.encode('utf-8')
	k = Key( bucket )
	k.key = 'data/' + target_path
	#print data_string
	k.set_contents_from_string(data_string)
	print k

def upload_file_to_s3( target_path, data_string ):
	""" 
	Uploads a string to S3, located in the sg14fbr bucket at the given path. 
	All of the data is put inside the 'data' folder on S3.  For example:
	target_path='twitter/06-25-14-300.json', data_string='{"some":"big","json":"object"}'
	This would place the json string into a file on S3 located at data/twitter/06-25-14-300.json
	"""
	conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
	#conn = connect_to_region(reg, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
	bucket = conn.get_bucket('cddl-riyadh-2')
	# for key in bucket.list():
	# 	print key.name.encode('utf-8')
	k = Key( bucket )
	k.key = 'data/' + target_path
	#print data_string
	k.set_contents_from_filename(data_string)
	print k

