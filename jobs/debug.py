# import json
# with open("2015-09-30+19-38-32.273000tweets.json") as file:	
#     data = file.read()
#     print data
#     print type(data)
#     print data[0]
#     new_data = json.loads(data)
#     print len(new_data)
#     print type(new_data)
#     
#     # print type(json.loads(new_data))

import cPickle as pickle

test = pickle.load(open( '2015-10-08+13-41-09.939000grams', "rb" ))
print test
