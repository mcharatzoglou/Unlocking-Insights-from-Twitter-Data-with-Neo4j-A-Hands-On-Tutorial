import py2neo
import pandas as pd
import numpy as np


port = input("Enter Neo4j DB Bolt port: ")
user = input("Enter Neo4j DB Username: ")
pswd = input("Enter Neo4j DB Password: ")
#graph = Graph("bolt://localhost:7687", auth=("neo4j", "neo4j_auth")

graph = py2neo.Graph(port, auth=(user, pswd))


#Get the total number of retweets
query1 = "MATCH p=()-[r:RETWEETED]->() RETURN COUNT(*)"
retweets = graph.run(query1).evaluate()
print("The total number of retweets is:", retweets)


#Get the 20 most popular hashtags (case insensitive) in descending order
query2 = """
    MATCH p=()-[r:HAS_HASHTAG]->(n:Hashtag)
    RETURN n.tag AS popular_hashtags, COUNT(*) AS frequency
    ORDER BY frequency DESC
    LIMIT 20
"""
pop_hashtags = graph.run(query2).to_data_frame()
print('The 20 most popular hashtags in descending order are:')
print(pop_hashtags)


#Get the total number of URLs (unique)
query3 = "MATCH (n:URL) RETURN COUNT(n)"
urls = graph.run(query3).evaluate()
print("The total number of urls is:", urls)


#Get the 20 users with most followers in descending order
query4 = """
    MATCH (n:User)
    WHERE n.followers IS NOT NULL
    RETURN n.username AS username, n.followers as followers
    ORDER BY followers DESC
    LIMIT 20
"""
pop_users = graph.run(query4).to_data_frame()
print('The 20 users with most followers in descending order are:')
print(pop_users)


#5 Get the hour with the most tweets and retweets
query5 = """
MATCH (u:User)-[:TWEETED]->(t:Tweet)
WITH u, t, substring(t.created_at, 11, 2) AS hour
RETURN u, t, toInteger(hour) AS tweet_hour
UNION
MATCH (u:User)-[:RETWEETED]->(t:Tweet)
WITH u, t, substring(t.created_at, 11, 2) AS hour
RETURN u, t, toInteger(hour) AS tweet_hour
"""
tweet_hour_df = graph.run(query5).to_data_frame()
hour_count = tweet_hour_df.groupby(['tweet_hour']).count()['u'].values
ind = np.argmax(hour_count)
print('The  hour with the most tweets and retweets is the', ind,'th')


#Get the 20 users, in descending order, that have been mentioned the most
query6 = """
    MATCH p=()-[r:MENTIONED]->(n:User)
    RETURN n.username AS username, COUNT(*) AS number_of_mentions
    ORDER BY number_of_mentions DESC
    LIMIT 20
"""
most_mentioned_users = graph.run(query6).to_data_frame()
print('The 20 users, in descending order, that have been mentioned the most are:')
print(most_mentioned_users)


'''Get the top 20 tweets that have been retweeted the most and the persons that
posted them'''
query7 = """
    MATCH (n:Tweet)
    RETURN n.id as tweet_id, n.author as author_id, n.retweets as retweets
    ORDER BY retweets DESC
    LIMIT 20
"""
most_retweeted = graph.run(query7).to_data_frame()
print('The top 20 tweets that have been retweeted the most and the persons that posted them are:')
print(most_retweeted)



#Run PageRank on the mention network
query8_1 = """
CALL gds.graph.project.cypher(
    'mentionGraph',
    'MATCH (u:User) RETURN id(u) AS id',
    'MATCH (u:User)-[r:MENTIONED]-(u1:User) RETURN id(u) AS source, id(u1) AS target')
"""
query8_2 = """CALL gds.pageRank.stream('mentionGraph') YIELD nodeId, score
RETURN gds.util.asNode(nodeId).username AS username, score
ORDER BY score DESC LIMIT 10"""

mention_graph = graph.run(query8_1)
PR = graph.run(query8_2).to_data_frame()
print("The 10 highest PageRank values for the 'Mentioned' network are:", PR)
print("The most important user according to the highest PageRank value is:",
      PR.iloc[0].values[0])


'''Get the 20 users with most similar hashtags to 
the 6th important user(as the others used no hashtags)'''

def jaccard_sim(list1, list2):
    '''compute Jaccard similarity of two sets'''
    intersec = len(set(list1).intersection(set(list2)))
    union = len(set(list1).union(set(list2)))
    if union>0:
        return intersec / union
    else:
        return 0


def get_hashtags(name):
    '''get the hashtags used by 6th important user
    '''

    query = """
        MATCH (u:User)-[r:USED_HASHTAG]->(h:Hashtag)
        WHERE u.username = $name
        RETURN DISTINCT h.tag 
    """
    tags = graph.run(query,name = name)
    hashtags = [t["h.tag"] for t in tags]

    return hashtags

def get_most_similar_user(name):
    '''get the 20 users with most similar hashtags
    to the 6th important user'''

    user_tags = get_hashtags(name)
    query9 = '''MATCH (u:User)-[r:USED_HASHTAG]->(h:Hashtag)
    WHERE u.username <> $name
    RETURN u.username, COLLECT(h.tag) AS hashtags'''
    result = graph.run(query9, name=name)
    tag_sim,  users = list(), list()
    for r in result:
        other_user = r["u.username"]
        other_tags = r["hashtags"]
        if len(other_tags)>0:
            sim= jaccard_sim(user_tags, other_tags)
            tag_sim.append(sim)
            users.append(other_user)
    tag_sim, users = zip(*sorted(zip(tag_sim, users)))
        
    sim_user = users[-20:]
    print('the 20 users who used most similar hashtags to the 6th important user are:', sim_user)

get_most_similar_user('ToofaniBaba1')


'''Get the top 10 users who have posted the most tweets, 
along with the number of tweets they've posted.'''
query11 = """
MATCH (u:User)-[:TWEETED]->(t:Tweet)
WITH u, COUNT(t) AS number_of_tweets
RETURN u.username AS username, number_of_tweets
ORDER BY number_of_tweets DESC
LIMIT 10
"""

active_users = graph.run(query11).to_data_frame()
print('The top 10 users who have posted the most tweets, along with the number of tweets they have posted are:')
print(active_users)


#Get the volumes of each type of tweets (where None is a tweet)
query12 = """
MATCH (t:Tweet)
RETURN t.type AS type, COUNT(t) AS volume
"""
types = graph.run(query12).to_data_frame()
print('The volumes of each type of tweets are:')
print(types)


