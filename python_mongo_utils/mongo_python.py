import pymongo
import time
def set_mongo_connection(connection_string,database,collection):
    client = pymongo.MongoClient(connection_string)
    db = client[database]
    col = db[collection]
    return client,db,col


def get_node_users(collection):
    documents = collection.find({"includes.users": {"$exists": True}})
    user_list = []
    for obj in documents:
        for user in obj['includes']['users']:
            user_list.append({
                'id': user['id'],
                'followers': user['public_metrics']['followers_count'],
                'username': user['username'],
                # this is the date that the Twitter API logged the tweet
                'info_last_updated': obj['data']['created_at_converted']
            })
    # sort user_list by info_last_updated in descending order
    user_list.sort(key=lambda x: x['info_last_updated'], reverse=True)

    unique_users = {}
    for user in user_list:
        if user['id'] not in unique_users:
            unique_users[user['id']] = user

    # convert unique_users dictionary to a list of user objects
    distinct_list_by_id = list(unique_users.values())

    # logs
    # for item in distinct_list_by_id:
    #     print(item)
    # print(str(len(user_list)) + " users found")
    # print(str(len(distinct_list_by_id)) + " distinct users kept")

    return distinct_list_by_id


#get all the tweets (only the 0th tweet) of the collection
def get_node_tweets(collection):
    documents = collection.find({"includes.tweets": {"$exists": True}})
    twitter_list = []

    for obj in documents:
        tweet = obj['includes']['tweets'][0]
        reference_tweets_list = []
        reference_tweets = tweet.get('referenced_tweets',None)
        if reference_tweets is not None:
            for reference_tweet in reference_tweets:
                reference_tweets_list.append({
                    'type':reference_tweet['type'],
                    'tweet_id': reference_tweet['id']
                })
        else:
            reference_tweets_list = None


        twitter_list.append({
            'tweet_id': tweet['id'],
            'author_id': tweet['author_id'],
            'created_at': tweet['created_at'],
            'retweet_count': tweet['public_metrics']['retweet_count'],
            'referenced_tweets': reference_tweets_list,
        })

    return twitter_list


#get all the unique hashtags (only the hashtags from the 0th tweet) of the collection
def get_node_hashtags(collection):
    documents = collection.find({"includes.tweets.0.entities.hashtags": {"$exists": True}})
    # set() for unique hashtags
    hashtag_set = set()
    for obj in documents:
        for hashtag in obj['includes']['tweets'][0]['entities']['hashtags']:
            hashtag_set.add(hashtag['tag'])

    hashtag_list = list(hashtag_set)

    return hashtag_list


# RELATIONSHIPS

def get_relationship_has_hashtag(collection):
    documents = collection.find({"includes.tweets.0.entities.hashtags": {"$exists": True}})
    tweets_with_hashtags = []
    for obj in documents:
        tweet = obj['includes']['tweets'][0]
        hashtag_list = []
        for hashtag in tweet['entities']['hashtags']:
            hashtag_list.append(hashtag['tag'])
        tweets_with_hashtags.append({
            'tweet_id': tweet['id'],
            'hashtags': hashtag_list
        })

    return tweets_with_hashtags

#insert data in Neo4j
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


def input_hashtags(data):
    '''Create nodes with "Hashtag" label in Neo4j'''

    for i in range(len(data)):
        node = py2neo.Node("Hashtag",tag=data[i])
        graph.create(node)


def input_tweets_tag_rel(rel_data):
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


#list of dicts for tweets
twitter_list  = get_node_tweets(collection)
print(twitter_list[0]['referenced_tweets'][0])
print(len(twitter_list))
#create Nodes of tweets in Neo4j
input_tweets(twitter_list)

#list of hashtags
hashtag_list  = get_node_hashtags(collection)
print(len(hashtag_list))
#create Nodes of hashtags in Neo4j
input_hashtags(hashtag_list)

#list of dicts of tweet_ids and the corresponding hashtags
hashtag_rel = get_relationship_has_hashtag(collection)
print(len(hashtag_rel))
#create Relationship HAS_HASHTAG in Neo4j
input_tweets_tag_rel(hashtag_rel)
