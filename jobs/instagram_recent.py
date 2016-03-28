from instagram.models import Media
from instagram.client import InstagramAPI
from datetime import datetime
import time, threading
import cPickle as pickle
from instagram_key import access_token, client_secret
from s3 import upload_to_s3

def get_media(locations, all_ids=[]):
   all_media = []
   for location in locations:
      recent_media = api.location_recent_media(location_id=location.id) 
      for media in recent_media:
         if isinstance(media, list) and len(media)>0:
            for m in media:
               if m.id not in all_ids:
                  all_media.append(m)
   return all_media
            
def get_lots_of_grams( locations ):
    """ Does pretty much what its long name suggests. """
    all_ids = []
    all_grams = []
    total_time = 600
    remaining_seconds = total_time
    interval = 30
    
    while remaining_seconds > 0:
        added = 0
        new_grams = get_media( locations, all_ids )
        for gram in new_grams:
            if gram.id not in all_ids:
                all_ids.append(gram.id)
                all_grams.append(gram)
                added+=1
                print gram
        print "At %d seconds, added %d new grams, for a total of %d" % ( total_time - remaining_seconds, added, len( all_grams ) )
        time.sleep(interval)
        remaining_seconds -= interval
    return all_grams
            
def save_object(obj, filename):
    with open(filename, 'wb') as output:
        pickle.dump(obj, output, -1)

def run():
   try:
       lat, lon = 24.6333, 46.7167
       locations = api.location_search(lat=lat, lng=lon, distance=5000, count=5000)
       target_path = 'instagram/%sgrams' %(str(datetime.now()))
       t = get_lots_of_grams(locations)
       
       new_t = pickle.dumps(t, -1)
       upload = upload_to_s3( target_path, new_t)
   except:
       pass
   threading.Timer(300, run).start()
   
api = InstagramAPI(access_token=access_token, client_secret=client_secret)   
run()




   
