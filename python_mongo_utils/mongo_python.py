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