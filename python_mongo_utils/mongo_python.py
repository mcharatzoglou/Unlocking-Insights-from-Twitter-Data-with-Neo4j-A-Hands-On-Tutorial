import pymongo

def set_mongo_connection(connection_string,database,collection):
    client = pymongo.MongoClient(connection_string)
    db = client[database]
    col = db[collection]
    return client,db,col


def get_users(collection):
    documents = collection.find({"includes.users": {"$exists": True}})
    get_user_list = []
    for item in documents:
        for user in item['includes']['users']:
            get_user_list.append({
                'id': user['id'],
                'followers': user['public_metrics']['followers_count']
            })
    return get_user_list


client,database,collection = set_mongo_connection(
    "mongodb://localhost:27017/",
    "local",
    "test"
)

users = get_users(collection)
for user in users:
    print(user)



