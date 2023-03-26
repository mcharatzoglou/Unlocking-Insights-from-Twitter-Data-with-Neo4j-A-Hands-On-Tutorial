from py2neo import Graph
from mongo_python import set_mongo_connection, get_node_users, get_node_tweets, get_node_hashtags, get_relationship_has_hashtag

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
        node = py2neo.Node("Hashtag", tag=data[i])
        graph.create(node)


def input_tweets_tag_rel(rel_data):
    '''Create Relationship with "HAS_HASHTAG" label in Neo4j'''

    for tweet in rel_data:
        tweet_id = tweet["tweet_id"]
        tweet_node = py2neo.Node("Tweet", id=tweet_id)
        graph.merge(tweet_node, "Tweet", "id")  # merge tweet nodes based on id

        for hashtag in tweet["hashtags"]:
            hashtag_node = py2neo.Node("Hashtag", tag=hashtag)
            graph.merge(hashtag_node, "Hashtag", "tag")  # merge hashtag nodes based on tag

            rel = py2neo.Relationship(tweet_node, "HAS_HASHTAG", hashtag_node)
            graph.create(rel)



#  SAMPLE CODE
client,database,collection = set_mongo_connection(
    "mongodb://localhost:27017/",
    "local",
    "test"
)

#relationships
has_hashtag = get_relationship_has_hashtag(collection)


# insert data in Neo4j
# port = input("Enter Neo4j DB Bolt port: ")
# user = input("Enter Neo4j DB Username: ")
# pswd = input("Enter Neo4j DB Password: ")
graph = Graph("bolt://localhost:7687", auth=("neo4j", "neo4j_auth"))

# graph = py2neo.Graph(port, auth=(user, pswd))

# list of dicts for tweets
twitter_list = get_node_tweets(collection)
print(twitter_list[0]['referenced_tweets'][0])
print(len(twitter_list))
# create Nodes of tweets in Neo4j
input_tweets(twitter_list)

# list of hashtags
hashtag_list = get_node_hashtags(collection)
print(len(hashtag_list))
# create Nodes of hashtags in Neo4j
input_hashtags(hashtag_list)

# list of dicts of tweet_ids and the corresponding hashtags
hashtag_rel = get_relationship_has_hashtag(collection)
print(len(hashtag_rel))
# create Relationship HAS_HASHTAG in Neo4j
input_tweets_tag_rel(hashtag_rel)