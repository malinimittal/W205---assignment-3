import json
from pymongo import MongoClient
from pymongo import ASCENDING
from dateutil import parser
import datetime

def getDb():
    client = MongoClient('mongodb://localhost:27017/')
    db = client.test_database
    return db

def dropCollection(collection):
    collection.remove({})

def userTweetCount(tweets, outdb):
    #count the number of tweets per user, and sort
    pipe = [
        { '$group': {'_id': '$userscreenname', 'total' : {'$sum' : 1}}},
        { '$sort': {'total': -1}},
        { '$out': outdb}
    ]
    tweets.aggregate(pipeline=pipe)

def hashtagCount(tweets, outdb):
    #count the number of times each hashtag occurs, and sort
    pipe = [
        { '$project': {'_id': 0, 'hashtags': '$userhashtags'}},
        { '$unwind' : '$hashtags'},
        { '$group': {'_id': '$hashtags', 'total': {'$sum': 1}}},
        { '$sort': {'total': -1}},
        { '$out': outdb}
    ]
    tweets.aggregate(pipeline=pipe)

def cleanData(jdoc):
    l = []
    for js in jdoc:
        d = {}
        d['created_at'] = parser.parse(js['created_at']).isoformat()
        d['userhashtags'] = [h[u'text'] for h in js['entities']['hashtags']]
        d['userscreenname'] = js['user']['screen_name'].encode('utf-8')
        d['username'] = js['user']['name'].encode('utf-8')
        l.append(d)
    return l

def getNumTweets(tweets, timestart, timeend):
    return tweets.find({"created_at" : { "$gte" : timestart, "$lt" : timeend}}).count()

if __name__ == "__main__":
    file1 = open("../prague-2015-02-14.json", 'r')
    jdoc1 = json.load(file1)

    file2 = open("../prague-2015-02-15.json", 'r')
    jdoc2 = json.load(file2)

    db = getDb()
    tweets = db.test_tweet_collection

    tweets.insert(cleanData(jdoc1))
    tweets.insert(cleanData(jdoc2))

    tweets.ensure_index("userscreenname", ASCENDING)
    tweets.ensure_index("created_at", ASCENDING)

    userTweetCount(tweets, "users")
    users = db.users
    print "users:"
    for user in users.find().limit(5):
        print user
    print

    print "hashtags:"
    hashtagCount(tweets, "hashtags")
    hashtags = db.hashtags
    for hashtag in hashtags.find().limit(10):
        print hashtag
    print

    date1 = datetime.datetime.strptime("2015-02-14T08:00:00", "%Y-%m-%dT%H:%M:%S")
    date2 = date1 + datetime.timedelta(days=1)
    for day in (date1, date2):
        for hour in range(0,7):
            starttime = day + datetime.timedelta(hours=hour)
            endtime = day + datetime.timedelta(hours=hour+1)
            numtweets = getNumTweets(tweets, starttime.isoformat(), endtime.isoformat())
            print ("Number of tweets on {0} between {1} and {2} (GMT) = {3}").format(starttime.date(), starttime.time(), endtime.time(), numtweets)

    # drop collections
    dropCollection(tweets)
    dropCollection(users)
    dropCollection(hashtags)
