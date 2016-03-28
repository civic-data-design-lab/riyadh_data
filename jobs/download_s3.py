import boto
import sys, os
from boto.s3.key import Key
from s3_key import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

datatype = 'foursquare'
LOCAL_PATH = '/' + datatype + '/'

bucket_name = 'cddl-riyadh-2'
# connect to the bucket
conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
bucket = conn.get_bucket(bucket_name)
# go through the list of files
bucket_list = bucket.list()#'data/'+ datatype)
for i, key in enumerate(bucket_list):
  #key_string = str(l.key)

  # check if file exists locally, if not: download it
  # if not os.path.exists(LOCAL_PATH):#+key_string):
  #   key.get_contents_to_filename(key.name)#'2015-10-14 23:55:18.011746foursquare_trending.json')
  if not os.path.exists('data'):
      os.makedirs('data')
  if i ==1:  
    if not os.path.exists('2015-10-14 23:55:18.011746foursquare_trending.json'):
        with open('test', 'wb') as temp_file:
            key.get_contents_to_file(temp_file)
      # key.get_contents_to_filename(key.name)#self.LOCAL_PATH+filename)
      # if _debug:
      #     print "Downloaded from bucket: "#+filename
  # check so file is downloaded, if so: delete from bucket
  # if os.path.exists(self.LOCAL_PATH+filename):
  #     key_list.delete()
  #     if _debug:
  #         print "Deleted from bucket:    "+filename

def download(bucket, filename):
    key = Key(bucket, filename)
    headers = {}
    mode = 'wb'
    updating = False
    if os.path.isfile(filename):
        mode = 'r+b'
        updating = True
        print "File exists, adding If-Modified-Since header"
        modified_since = os.path.getmtime(filename)
        timestamp = datetime.datetime.utcfromtimestamp(modified_since)
        headers['If-Modified-Since'] = timestamp.strftime("%a, %d %b %Y %H:%M:%S GMT")
    try:
        with open(filename, mode) as f:
            key.get_contents_to_file(f, headers)
            f.truncate()
    except boto.exception.S3ResponseError as e:
        if not updating:
            # got an error and we are not updating an existing file
            # delete the file that was created due to mode = 'wb'
            os.remove(filename)
        return e.status
    return 200
  
class get_logs:
    """Download log files from the specified bucket and path and then delete them from the bucket.
    Uses: http://boto.s3.amazonaws.com/index.html
    """
    # Set default values
    AWS_BUCKET_NAME = '{bucket}'
    AWS_KEY_PREFIX = '{prefix}'
    AWS_ACCESS_KEY_ID = '{access key}'
    AWS_SECRET_ACCESS_KEY = '{secret key}'
    LOCAL_PATH = '{local path}'
    # Don't change below here
    s3_conn = None
    bucket_list = None

    def __init__(self):
        s3_conn = None
        bucket_list = None
        
def copyFiles(self):
    """Creates a local folder if not already exists and then download all keys and deletes them from the bucket"""
    # Using makedirs as it's recursive
    if not os.path.exists(self.LOCAL_PATH):
        os.makedirs(self.LOCAL_PATH)
    for key_list in self.bucket_list:
        key = str(key_list.key)
        # Get the log filename (L[-1] can be used to access the last item in a list).
        filename = key.split('/')[-1]
        # check if file exists locally, if not: download it
        if not os.path.exists(self.LOCAL_PATH+filename):
            key_list.get_contents_to_filename(self.LOCAL_PATH+filename)
            if _debug:
                print "Downloaded from bucket: "+filename
        # check so file is downloaded, if so: delete from bucket
        if os.path.exists(self.LOCAL_PATH+filename):
            key_list.delete()
            if _debug:
                print "Deleted from bucket:    "+filename