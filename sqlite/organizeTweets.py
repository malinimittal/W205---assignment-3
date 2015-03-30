import sqlite3 as lite
import sys
import os
import json
from dateutil import parser
import datetime

numUsers = 0
numHashtags = 0

def connectToDataBase(db):
    try:
        return lite.connect(db)
    except lite.Error, e:
        print e
        return None

def createUsersTable(cursor):
    cursor.execute("CREATE TABLE Users(id INT PRIMARY KEY, name TEXT, screen_name TEXT NOT NULL)")

def createHashtagsTable(cursor):
    cursor.execute("CREATE TABLE Hashtags(id INT PRIMARY KEY, name TEXT NOT NULL)")

def createTweetsTable(cursor):
    cursor.execute("CREATE TABLE Tweets(id INT PRIMARY KEY, date TEXT, user_id INT, FOREIGN KEY(user_id) REFERENCES Users(id))")

def createTweetsHashtagsTable(cursor):
    cursor.execute("CREATE TABLE Tweets_Hashtags(tweet_id INT, hashtag_id INT, FOREIGN KEY(tweet_id) REFERENCES Tweets(id), FOREIGN KEY(hashtag_id) REFERENCES Hashtags(id))")

def createTables(cursor):
    createUsersTable(cursor)
    createHashtagsTable(cursor)
    createTweetsTable(cursor)
    createTweetsHashtagsTable(cursor)

def addUser(name, screenname, cursor):
    query = ''.join(['SELECT * FROM Users WHERE screen_name="', screenname, '"'])
    cursor.execute(query)

    data = cursor.fetchone()
    if data == None:
        global numUsers
        numUsers += 1
        query = ''.join(['INSERT INTO Users VALUES(', str(numUsers), ',"', name, '","', screenname,'")'])
        cursor.execute(query)
        return numUsers
    else:
        return data[0]

def addTweet(id, create_date, user_id, cursor):
    query = ''.join(['INSERT INTO Tweets VALUES(', str(id), ',"', create_date, '",', str(user_id),')'])
    cursor.execute(query)

def addHashtag(name, tweet_id, cursor):
    query = ''.join(['SELECT * FROM Hashtags WHERE name="', name, '"'])
    cursor.execute(query)

    data = cursor.fetchone()
    hash_id = 0
    if data == None:
        global numHashtags
        numHashtags += 1
        query = ''.join(['INSERT INTO Hashtags VALUES(', str(numHashtags), ',"', name, '")'])
        cursor.execute(query)
        hash_id = numHashtags
    else:
        hash_id = data[0]

    query = ''.join(['INSERT INTO Tweets_Hashtags VALUES(', str(tweet_id), ',', str(hash_id), ')'])
    cursor.execute(query)

def addDataToTables(jdoc, cursor):
    for js in jdoc:
        username = js['user']['name'].encode('utf-8')
        userscreenname = js['user']['screen_name'].encode('utf-8')
        user_id = addUser(username, userscreenname, cursor)
        addTweet(js['id'], parser.parse(js['created_at']).isoformat(), user_id, cursor)
        hashTags = js['entities']['hashtags']
        for tag in hashTags:
            addHashtag(tag['text'], js['id'], cursor)

def printUserWithMostTweets(cursor, num):
    query = 'Select user_id, Count(*) from Tweets Group by user_id ORDER by Count(*) DESC'
    cursor.execute(query)

    cursor2 = conn.cursor()
    for i in range(0, num):
        row = cursor.fetchone()
        query = ''.join(['Select * from Users where id=', str(row[0])])
        cursor2.execute(query)
        print ("total = {0}, screen_name = {1}").format(row[1], cursor2.fetchone()[2])

def printMostUsedHashtags(cursor, num):
    query = 'Select hashtag_id, Count(*) from Tweets_Hashtags Group by hashtag_id ORDER by Count(*) DESC'
    cursor.execute(query)

    cursor2 = conn.cursor()
    for i in range(0, num):
        row = cursor.fetchone()
        query = ''.join(['Select * from Hashtags where id=', str(row[0])])
        cursor2.execute(query)
        print ("total = {0}, hashtag = {1}").format(row[1], cursor2.fetchone()[1])

def getNumTweets(cursor, starttime, endtime):
    query = ''.join(['Select Count(*) from Tweets where date >= "', starttime, '" and date < "', endtime, '"'])
    cursor.execute(query)
    data = cursor.fetchone()
    if data == None:
        return 0
    return data[0]

if __name__ == "__main__":
    dbname = "tweets.db"
    rmstr = "rm -f " + dbname
    os.system(rmstr)
    conn = connectToDataBase(dbname)
    if conn == None:
        sys.exit(1)

    cursor = conn.cursor()
    createTables(cursor)

    jdoc1 = json.load(open("../prague-2015-02-14.json", 'r'))
    jdoc2 = json.load(open("../prague-2015-02-15.json", 'r'))

    addDataToTables(jdoc1, cursor)
    addDataToTables(jdoc2, cursor)

    conn.commit()

    print "Users:"
    printUserWithMostTweets(cursor, 5)
    print "\nHashtags:"
    printMostUsedHashtags(cursor, 10)
    print

    date1 = datetime.datetime.strptime("2015-02-14T08:00:00", "%Y-%m-%dT%H:%M:%S")
    date2 = date1 + datetime.timedelta(days=1)
    for day in (date1, date2):
        for hour in range(0,7):
            starttime = day + datetime.timedelta(hours=hour)
            endtime = day + datetime.timedelta(hours=hour+1)
            numtweets = getNumTweets(cursor, starttime.isoformat(), endtime.isoformat())
            print ("Number of tweets on {0} between {1} and {2} (GMT) = {3}").format(starttime.date(), starttime.time(), endtime.time(), numtweets)

    cursor.close()
    conn.close()
