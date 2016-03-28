#!/usr/bin/env python

from boto.s3.connection import S3Connection
from boto.s3.key import Key

import os
import json
import time
import math

from py2neo.ext.spatial.exceptions import GeometryExistsError
from py2neo import neo4j, Node, Relationship, Graph
from py2neo import * # only really needed for WriteBatch
from py2neo.ext.spatial import Spatial
from py2neo.ext.spatial.util import parse_lat_long

from os import listdir
from os.path import isfile, join

from neo4j_key import DB_NAME, PW, HOST

#import esm
#import esmre 	# using esmre and not just esm to take advantage of regexes

# set up authentication parameters
authenticate("localhost:7474", DB_NAME, PW)

DB = Graph( HOST )
# The layer is the index
spatial = Spatial(DB)

def push_all_tweets_to_db( all_tweets):
    tweets_count = 0
    graph = Graph()
    cypher = graph.cypher
    for tid, tweet in all_tweets.iteritems():
        #push_tweet_to_db( tid, tweet, point_idx )
        create_twitter_node(tid, tweet, cypher)
        tweets_count += 1
    print "Added %d tweets to the db." % tweets_count 

def create_twitter_node(tid, tweet, cypher):
    tweet_props = { k: v for k,v in tweet.iteritems() if k != 'raw_source' }
    tweet_props['lat'], tweet_props['lon'] = tweet_props['lon'], tweet_props['lat']    # twitter.py accidentally is swapping lattitude and longitude, swap it back here
    if tweet['raw_source']['in_reply_to_user_id_str'] == None: tweet_props['in_reply_to_user_id_str'] = ''
    else: tweet_props['in_reply_to_user_id_str'] = tweet['raw_source']['in_reply_to_user_id_str']
    if tweet['raw_source']['in_reply_to_status_id_str'] == None: tweet_props['in_reply_to_status_id_str'] = ''
    else: tweet_props['in_reply_to_status_id_str'] = tweet['raw_source']['in_reply_to_status_id_str']
    tweet_props['time'] = tweet['raw_source']['created_at']
    tweet_props['origin'] = 'twitter'
    tweet_props['raw_source'] = json.dumps(tweet)
    
    raw_user = tweet['raw_source']['user']
    user_props = {
        'username': raw_user['screen_name'],
        'followers_count': raw_user['followers_count'],
        'id_str': raw_user['id_str'],
        'location': raw_user['location'],
        'lang': raw_user['lang'],
        'name': raw_user['name'],
        'description': raw_user['description']
    }
    tweet_props['user'] = user_props['username']
    # "in_reply_to_user_id":2570000839
    # "in_reply_to_user_id_str":"2570000839"
    # "in_reply_to_status_id":651952261241962496
    # "in_reply_to_status_id_str":"651952261241962496",
    # "retweeted":false,
    query_string = """ 
            MERGE (tweet:Social:Tweets {
                lat: {lat}, lon: {lon}, in_reply_to_user_id_str: {in_reply_to_user_id_str},
                in_reply_to_status_id_str: {in_reply_to_status_id_str}, time:{time}, origin:{origin},
                raw_source: {raw_source}, user: {user}, content: {content}, tweet_id: {tweet_id}
            })
            MERGE (user:Users:TwitterUsers {  
                username: {username},
                followers_count: {followers_count},
                id_str: {id_str},
                location: {location},
                lang: {lang},
                name: {name},
                description: {description}
            })
        """
    cypher.execute(query_string, lat=tweet_props['lat'], lon=tweet_props['lon'], in_reply_to_user_id_str=tweet_props['in_reply_to_user_id_str'],
                   in_reply_to_status_id_str=tweet_props['in_reply_to_status_id_str'], time=tweet_props['time'], origin=tweet_props['origin'],
                   raw_source=tweet_props['raw_source'], user=tweet_props['user'], content=tweet_props['content'], tweet_id=tweet_props['tweet_id'],
                   username=user_props['username'], followers_count=user_props['followers_count'], id_str=user_props['id_str'],
                   location=user_props['location'], lang=user_props['lang'], name=user_props['name'], description=user_props['description'])
    
    'delete all'
    # MATCH (n)
    # OPTIONAL MATCH (n)-[r]-()
    # DELETE n,r

def create_spatial_tweets(spatial_layer_name, labels_string):
    spatial.create_layer(spatial_layer_name)
    tweet_query = "MATCH ({}) RETURN Social;".format(labels_string)
    records = DB.cypher.execute(tweet_query)
    for record in records:
        node = record[0]
        node_id = node._id
        properties = node.properties
        lat = properties['lat']
        lon = properties['lon']
        tweet_loc = (lat, lon)
        shape = parse_lat_long(tweet_loc)
        
        tweet_id = properties['tweet_id']
        try:
            spatial.create_geometry(geometry_name=tweet_id, wkt_string=shape.wkt, layer_name="Spatial_Tweets", node_id=node_id)
            print('created {}'.format(tweet_id))
        except GeometryExistsError:
            print 'The geometry is already in the DB'
            
def get_node_by_label_property(label, prop, prop_val):
    query = "MATCH (n:%s {%s:'%s'}) RETURN n" %(label, prop, prop_val)
    records = DB.cypher.execute(query)
    return records
            
def get_node_name(label):
    if ':' in label:
        index = label.index(':')
        label = label[:index]
    return label
        
def add_relationship(label_a, label_b, property_a, property_b):
    data_name = get_node_name(label_a)
    query = "MATCH ({}) RETURN {};".format(label_a, data_name)
    
    records = DB.cypher.execute(query)
    for record in records:
        node = record[0]
        node_id = node._id
        properties = node.properties
        value_a = properties[property_a]
        if value_a != '':
            other_nodes = get_node_by_label_property(label_b, property_b, value_a)
            print len(other_nodes)

# This add the relationships between tweets and users tweeting
def add_user_tweet_relation(label_a, label_b, property_a, property_b):
    graph = Graph()
    cypher = graph.cypher
    
    tweet_query = "MATCH ({}) RETURN user;".format(label_a)
    records = DB.cypher.execute(tweet_query)
    for record in records:
        node = record[0]
        node_id = node._id
        properties = node.properties
        value_a = properties[property_a]
        other_nodes = get_node_by_label_property(label_b, property_b, value_a)
        if len( other_nodes ) > 0:
            for other_node in other_nodes:
                # t_time = str(other_node[0]['time'])
                # query_string = 'MERGE (%s)-[r:Tweeted {tweet_time: %s}]->(%s)' %(record[0], t_time, other_node[0])
                # print query_string
                # cypher.execute(query_string)
                relationship = Relationship(record[0], 'Tweeted', other_node[0], tweet_time=other_node[0]['time'])
                #http://localhost:7474/browser/
                #print relationship.exists
                DB.create(relationship)

def all_tweets_s3_to_neo(tweet_jsons, file_path):
    for key in tweet_jsons:
    # for key in bucket.list( 'data/twitter' ):
        with open(join(file_dir,key)) as file:	
            raw_data = file.read()
        #raw_data = key.get_contents_as_string()
            if raw_data == '': continue
            print 'Processing tweets from %s...' % key
            data = json.loads( raw_data )
            push_all_tweets_to_db( data )
            
    'Create Spatial Index'
    create_spatial_tweets("Spatial_Tweets", 'Social:Tweets')
    
    'Create Users-Tweet Relationships'
    add_user_tweet_relation('user:Users:TwitterUsers', 'Social:Tweets', 'username', 'user')
    add_relationship('tweet:Social:Tweets', 'tweet:Social:Tweets', 'in_reply_to_status_id_str', 'tweet_id')
    add_relationship('tweet:Social:Tweets', 'user:Users:TwitterUsers', 'in_reply_to_user_id_str', 'id_str')
    
    # MATCH (ee:Person) WHERE ee.name = "Emil" RETURN ee;
    #MATCH (ee:Person)-[:KNOWS]-(friends)
    #WHERE ee.name = "Emil" RETURN ee, friends
    #START a=node(3)
    #MATCH (a)-[:KNOWS*]->(d)
    #RETURN distinct d

if __name__=="__main__":
    file_dir = "F:/Dropbox (MIT)/Independent Study Sarah/Riyadh Data/01. JSON/twitter"
    #file_dir = "C:/Users/mitadm/Dropbox (MIT)/Independent Study Sarah/Riyadh Data/01. JSON/twitter"
    onlyfiles = [ f for f in listdir(file_dir) if isfile(join(file_dir,f)) and not f.startswith('.')]#[:1000]
    
    all_tweets_s3_to_neo(onlyfiles, file_dir)
    
    print 'Finished processing tweets.'