import pymongo


def set_mongo_connection(connection_string,database,collection):
    client = pymongo.MongoClient(connection_string)
    db = client[database]
    col = db[collection]
    return client,db,col


def get_users(collection):
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



