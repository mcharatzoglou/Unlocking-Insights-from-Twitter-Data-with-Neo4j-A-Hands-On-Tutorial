import pymongo
import time
def set_mongo_connection(connection_string,database,collection):
    client = pymongo.MongoClient(connection_string)
    db = client[database]
    col = db[collection]
    return client,db,col

#get distinct users that exists in users.0 and all the users in tweets mentions
def get_node_users(collection):
    documents = collection.find({"includes.users": {"$exists": True}})
    user_list = []
    user_ids_to_remove = set()
    for obj in documents:
        for user in obj['includes']['users']:
            user_list.append({
                'id': user['id'],
                'followers': user['public_metrics']['followers_count'] if 'public_metrics' in user else None,
                'username': user['username'],
                # this is the date that the Twitter API logged the tweet
                'info_last_updated': obj['data']['created_at_converted']
            })
            # for keeping the users that found in includes.users
            user_ids_to_remove.add(user['id'])

    # add the users in 'mentions' that are not in 'users'
    documents = collection.find({"includes.tweets.0.entities.mentions": {"$exists": True}})
    for obj in documents:
        for user in obj['includes']['tweets'][0]['entities']['mentions']:
            # if the user already in user_list don't append him
            if user['id'] not in user_ids_to_remove:
                user_list.append({
                    'id': user['id'],
                    'followers': None,
                    'username': user['username'],
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
    count_all_hashtags = 0
    hashtag_set = set()
    for obj in documents:
        for hashtag in obj['includes']['tweets'][0]['entities']['hashtags']:
            count_all_hashtags +=1
            hashtag_set.add(hashtag['tag'])

    hashtag_list = list(hashtag_set)

    # print(str(count_all_hashtags) + " hashtags found")
    # print(str(len(hashtag_set)) + " distinct hashtags kept")

    return hashtag_list

def get_node_urls(collection):
    documents = collection.find({"includes.tweets.0.entities.urls": {"$exists": True}})
    # set() for unique urls
    count_all_urls = 0
    url_set = set()
    for obj in documents:
        for url in obj['includes']['tweets'][0]['entities']['urls']:
            count_all_urls +=1
            url_set.add(url['url'])

    url_list = list(url_set)

    # print(str(count_all_urls) + " urls found")
    # print(str(len(url_set)) + " distinct urls kept")

    return url_list


# RELATIONSHIPS
def get_relationship_tweeted(collection):
    documents = collection.find({"includes.tweets.0": {"$exists": True}})
    user_tweeted_tweet = {}

    for obj in documents:
        user_id = obj['includes']['users'][0]['id']
        tweet_id = obj['includes']['tweets'][0]['id']
        created_at_converted = obj['includes']['tweets'][0]['created_at_converted']

        if user_id not in user_tweeted_tweet:
            user_tweeted_tweet[user_id] = {
                'id': user_id,
                'tweet_ids': [],
                'created_at_converted': []
            }

        user_tweeted_tweet[user_id]['tweet_ids'].append(tweet_id)
        user_tweeted_tweet[user_id]['created_at_converted'].append(created_at_converted)

    return list(user_tweeted_tweet.values())

def get_relationship_has_hashtag(collection):
    documents = collection.find({"includes.tweets.0.entities.hashtags": {"$exists": True}})
    tweets_with_hashtags = []
    unique_ids = set()
    for obj in documents:
        tweet = obj['includes']['tweets'][0]
        tweet_id = tweet['id']
        if tweet_id not in unique_ids:
            unique_ids.add(tweet_id)
            hashtag_list = []
            for hashtag in tweet['entities']['hashtags']:
                hashtag_list.append(hashtag['tag'])
            tweets_with_hashtags.append({
                'tweet_id': tweet_id,
                'hashtags': hashtag_list
            })

    return tweets_with_hashtags

def get_relationship_has_url(collection):
    documents = collection.find({"includes.tweets.0.entities.urls": {"$exists": True}})
    tweets_with_urls = []
    unique_ids = set()
    for obj in documents:
        tweet = obj['includes']['tweets'][0]
        tweet_id = tweet['id']
        if tweet_id not in unique_ids:
            unique_ids.add(tweet_id)
            url_list = []
            for url in tweet['entities']['urls']:
                url_list.append(url['url'])
            tweets_with_urls.append({
                'tweet_id': tweet_id,
                'urls': url_list
            })

    return tweets_with_urls


def get_relationship_used_hashtag(collection):
    documents = collection.find({"includes.tweets.0.entities.hashtags": {"$exists": True}})
    user_used_hashtags = {}
    for obj in documents:
        tweet = obj['includes']['tweets'][0]
        user_id = obj['includes']['users'][0]['id']
        if user_id not in user_used_hashtags:
            hashtag_set = set()
            for hashtag in tweet['entities']['hashtags']:
                hashtag_set.add(hashtag['tag'])
            hashtag_list = list(hashtag_set)
            user_used_hashtags[user_id] = {
                'id': user_id,
                'hashtags': hashtag_list
            }
        else:
            for hashtag in tweet['entities']['hashtags']:
                user_used_hashtags[user_id]['hashtags'].append(hashtag['tag'])
            user_used_hashtags[user_id]['hashtags'] = list(set(user_used_hashtags[user_id]['hashtags']))

    # convert dictionary to list
    user_used_hashtags = list(user_used_hashtags.values())

    return user_used_hashtags


def get_relationship_used_urls(collection):
    documents = collection.find({"includes.tweets.0.entities.urls": {"$exists": True}})
    user_used_urls = {}
    for obj in documents:
        tweet = obj['includes']['tweets'][0]
        user_id = obj['includes']['users'][0]['id']
        if user_id not in user_used_urls:
            url_set = set()
            for url in tweet['entities']['urls']:
                url_set.add(url['url'])
            url_list = list(url_set)
            user_used_urls[user_id] = {
                'id': user_id,
                'urls': url_list
            }
        else:
            for url in tweet['entities']['urls']:
                user_used_urls[user_id]['urls'].append(url['url'])
            user_used_urls[user_id]['urls'] = list(set(user_used_urls[user_id]['urls']))

    # convert dictionary to list
    user_used_urls = list(user_used_urls.values())

    return user_used_urls


def get_relationship_mentioned(collection):
    documents = collection.find({"includes.tweets.0.entities.mentions": {"$exists": True}})
    user_mentioned_user = {}
    for obj in documents:
        tweet = obj['includes']['tweets'][0]
        user_id = obj['includes']['users'][0]['id']
        if user_id not in user_mentioned_user:
            mention_set = set()
            for mention in tweet['entities']['mentions']:
                mention_set.add(mention['id'])
            mention_list = list(mention_set)
            user_mentioned_user[user_id] = {
                'id': user_id,
                'mentions': mention_list
            }
        else:
            for mention in tweet['entities']['mentions']:
                user_mentioned_user[user_id]['mentions'].append(mention['id'])
            user_mentioned_user[user_id]['mentions'] = list(set(user_mentioned_user[user_id]['mentions']))

    # convert dictionary to list
    user_mentioned_user = list(user_mentioned_user.values())

    return user_mentioned_user