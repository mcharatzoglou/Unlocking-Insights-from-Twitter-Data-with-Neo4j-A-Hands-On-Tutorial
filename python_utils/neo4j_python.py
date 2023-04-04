import py2neo
from  mongo_python import *

client,database,collection = set_mongo_connection(
    "mongodb://localhost:27017/",
    "twitter_db",
    "tweets"
)


port = input("Enter Neo4j DB Bolt port: ")
user = input("Enter Neo4j DB Username: ")
pswd = input("Enter Neo4j DB Password: ")
#graph = Graph("bolt://localhost:7687", auth=("neo4j", "neo4j_auth")

graph = py2neo.Graph(port, auth=(user, pswd))


def input_tweets(data):
    '''Create nodes with "Tweet" label in Neo4j'''

    for i in range(len(data)):
        node = py2neo.Node("Tweet", id=data[i]['tweet_id'], 
                           author=data[i]['author_id'], 
                           created_at=data[i]['created_at'],
                           retweets=data[i]['retweet_count'],
                           type=data[i]['referenced_tweets'][0]['type'],
                           ref_tweet=data[i]['referenced_tweets'][0]['tweet_id'])
        graph.create(node)
        #graph.merge(node, "Tweet", "id")


def input_hashtags(data):
    '''Create nodes with "Hashtag" label in Neo4j'''

    for i in range(len(data)):
        node = py2neo.Node("Hashtag",tag=data[i])
        graph.create(node)
        #graph.merge(node, "Hashtag", "tag")


def input_urls(data):
    '''Create nodes with "URL" label in Neo4j'''

    for i in range(len(data)):
        node = py2neo.Node("URL", address=data[i])
        graph.create(node)
        #graph.merge(node, "URL", "address")



def input_users(data):
    '''Create nodes with "Tweet" label in Neo4j'''

    for i in range(len(data)):
        node = py2neo.Node("User", id=data[i]['id'], 
                           followers=data[i]['followers'], 
                           username=data[i]['username'],
                           info_updated=data[i]['info_last_updated'])
        graph.create(node)
        #graph.merge(node, "User", "id")


#Relationships
def input_tweet_tag_rel(rel_data):
    '''Create Relationship with "HAS_HASHTAG" label in Neo4j'''

    for tweet in rel_data:
        tweet_id = tweet["tweet_id"]
        tweet_node = py2neo.Node("Tweet", id=tweet_id)
        graph.merge(tweet_node, "Tweet", "id") #merge tweet nodes based on id
        
        for hashtag in tweet["hashtags"]:
            hashtag_node = py2neo.Node("Hashtag", tag=hashtag)
            graph.merge(hashtag_node, "Hashtag", "tag") #merge hashtag nodes based on tag
            
            rel = py2neo.Relationship(tweet_node, "HAS_HASHTAG", hashtag_node)
            graph.create(rel)


def input_user_tag_rel(rel_data):
    '''Create Relationship with "USED_HASHTAG" label in Neo4j'''
    for user in rel_data:
    
        user_id = user["id"]
        user_node = py2neo.Node("User", id=user_id)
        graph.merge(user_node, "User", "id") #merge tweet nodes based on id
        
        for hashtag in user["hashtags"]:
            hashtag_node = py2neo.Node("Hashtag", tag=hashtag)
            graph.merge(hashtag_node, "Hashtag", "tag") #merge hashtag nodes based on tag
            
            rel = py2neo.Relationship(user_node, "USED_HASHTAG", hashtag_node)
            graph.create(rel)


def input_tweet_url_rel(rel_data):
    '''Create Relationship with "HAS_URL" label in Neo4j'''

    for tweet in rel_data:
        tweet_id = tweet["tweet_id"]
        tweet_node = py2neo.Node("Tweet", id=tweet_id)
        graph.merge(tweet_node, "Tweet", "id") #merge tweet nodes based on id
        
        for url in tweet["urls"]:
            url_node = py2neo.Node("URL", address=url)
            graph.merge(url_node, "URL", "address") #merge url nodes based on url
            
            rel = py2neo.Relationship(tweet_node, "HAS_URL", url_node)
            graph.create(rel)



def input_user_tweet_rel(rel_data):
    '''Create Relationship with "TWEETED" label in Neo4j'''
    for user in rel_data:

        user_id = user["id"]
        user_node = py2neo.Node("User", id=user_id)
        graph.merge(user_node, "User", "id") #merge user nodes based on id
        i = 0
        for tweet in user["tweeted"]:

            tweet_node = py2neo.Node("Tweet", id=tweet)
            graph.merge(tweet_node, "Tweet", "id") #merge url nodes based on url
            
            rel = py2neo.Relationship(user_node, "TWEETED", 
                        tweet_node, created_at=user["created_at_converted"][i])
            graph.create(rel)
            #graph.merge(rel, "TWEETED", f"{user_id}:{tweet}")
            i+=1
        
    


def input_user_retweet_rel(rel_data):
    '''Create Relationship with "RETWEETED" label in Neo4j'''
    for user in rel_data:

        user_id = user["id"]
        user_node = py2neo.Node("User", id=user_id)
        graph.merge(user_node, "User", "id") #merge tweet nodes based on id
        i = 0
        for tweet in user["retweeted"]:
            tweet_node = py2neo.Node("Tweet", id=tweet)
            graph.merge(tweet_node, "Tweet", "id") #merge url nodes based on url
            
            rel = py2neo.Relationship(user_node, "RETWEETED", 
                        tweet_node, created_at=user["created_at_converted"][i])
            graph.create(rel)
            i+=1


def input_user_quoted_rel(rel_data):
    '''Create Relationship with "QUOTED" label in Neo4j'''
    for user in rel_data:
   
        user_id = user["id"]
        user_node = py2neo.Node("User", id=user_id)
        graph.merge(user_node, "User", "id") #merge tweet nodes based on id
        i = 0
        for tweet in user["quoted"]:
            tweet_node = py2neo.Node("Tweet", id=tweet)
            graph.merge(tweet_node, "Tweet", "id") #merge url nodes based on url
            
            rel = py2neo.Relationship(user_node, "QUOTED", 
                        tweet_node, created_at=user["created_at_converted"][i])
            graph.create(rel)
            i+=1


def input_user_replied_to_rel(rel_data):
    '''Create Relationship with "REPLIED TO" label in Neo4j'''

    for user in rel_data:
        user_id = user["id"]
        user_node = py2neo.Node("User", id=user_id)
        graph.merge(user_node, "User", "id") #merge tweet nodes based on id
        i = 0
        for tweet in user["replied_to"]:
            tweet_node = py2neo.Node("Tweet", id=tweet)
            graph.merge(tweet_node, "Tweet", "id") #merge url nodes based on url
            
            rel = py2neo.Relationship(user_node, "REPLIED_TO", 
                        tweet_node, created_at=user["created_at_converted"][i])
            graph.create(rel)
            i+=1


def input_user_user_rel(rel_data):
    '''Create Relationship with "MENTIONED" label in Neo4j'''
    for user in rel_data:

        user_id = user["id"]
        user_node = py2neo.Node("User", id=user_id)
        graph.merge(user_node, "User", "id") #merge tweet nodes based on id
        
        for mentioned_user in user["mentions"]:
            mentioned_user_node = py2neo.Node("User", id=mentioned_user)
            graph.merge(mentioned_user_node, "User", "id") #merge url nodes based on url
            
            rel = py2neo.Relationship(user_node, "MENTIONED", mentioned_user_node)
            graph.create(rel)


#list of dicts for tweets
tweet_list  = get_node_tweets(collection)
#create Nodes of tweets in Neo4j
input_tweets(tweet_list)

#list of hashtags
hashtag_list  = get_node_hashtags(collection)
#create Nodes of hashtags in Neo4j
input_hashtags(hashtag_list)

#list of urls
url_list  = get_node_urls(collection)
#create Nodes of hashtags in Neo4j
input_urls(url_list)


#list of dicts for users
user_list  = get_node_users(collection)
#create Nodes of users in Neo4j
input_users(user_list)


#list of dicts of tweet_ids and the corresponding hashtags
tweet_hashtag_rel = get_relationship_has_hashtag(collection)
#create Relationship HAS_HASHTAG in Neo4j
input_tweet_tag_rel(tweet_hashtag_rel)

#list of dicts of tweet_ids and the corresponding urls
tweet_url_rel = get_relationship_has_url(collection)
#create Relationship HAS_URL in Neo4j
input_tweet_url_rel(tweet_url_rel)

#list of dicts of user_ids and the corresponding hashtags
user_hashtag_rel = get_relationship_used_hashtag(collection)
#create Relationship USED_HASHTAG in Neo4j
input_user_tag_rel(user_hashtag_rel)


#list of dicts of user_ids and the corresponding tweet ids
user_tweet_rel = get_relationship_tweeted(collection)
#create Relationship TWEETED in Neo4j
input_user_tweet_rel(user_tweet_rel)

#list of dicts of user_ids and the corresponding retweet ids
user_retweet_rel = get_relationship_retweeted(collection)
#create Relationship RETWEETED in Neo4j
input_user_retweet_rel(user_retweet_rel)

#list of dicts of user_ids and the corresponding quoted tweeets ids
user_quote_rel = get_relationship_quoted(collection)
#create Relationship QUOTED in Neo4j
input_user_quoted_rel(user_quote_rel)

#list of dicts of user_ids and the corresponding retweet ids
user_replied_to_rel = get_relationship_replied_to(collection)
#create Relationship REPLIED TO in Neo4j
input_user_replied_to_rel(user_replied_to_rel)

#list of dicts of user_ids and the corresponding mentioned user ids
user_user_rel = get_relationship_mentioned(collection)
#create Relationship MENTIONED in Neo4j
input_user_user_rel(user_user_rel)

