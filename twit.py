Spidering Twitter using a database
Using databases and Structured Query Language (SQL)

# We create a simple spidering program that will go through
# Twitter accounts and build a database of them. Note: Be very careful when running
# this program. You do not want to pull too much data or run the program for too
# long and end up having your Twitter access shut off.

# One of the problems of any kind of spidering program is that it needs to be able
# to be stopped and restarted many times and you do not want to lose the data that
# you have retrieved so far. You don’t want to always restart your data retrieval at
# the very beginning so we want to store data as we retrieve it so our program can
# start back up and pick up where it left off.

# We will start by retrieving one person’s Twitter friends and their statuses, looping
# through the list of friends, and adding each of the friends to a database to be
# retrieved in the future. After we process one person’s Twitter friends, we check
# in our database and retrieve one of the friends of the friend. We do this over and
# over, picking an “unvisited” person, retrieving their friend list, and adding friends
# we have not seen to our list for a future visit.

# We also track how many times we have seen a particular friend in the database to
# get some sense of their “popularity”.

# By storing our list of known accounts and whether we have retrieved the account
# or not, and how popular the account is in a database on the disk of the computer,
# we can stop and restart our program as many times as we like.

# This program is a bit complex. It is based on the code from the exercise earlier in
# the book that uses the Twitter API. Here is the source code for our Twitter
# spidering application:

import twurl
import json
import urllib
import sqlite3

TWITTER_URL = 'https://api.twitter.com/1.1/friends/list.json'

conn = sqlite3.connect('spider.sqlite23')
cur = conn.cursor()

# Our database is stored in the file spider.sqlite3 and it has one table named
# Twitter. Each row in the Twitter table has a column for the account name,
# whether we have retrieved the friends of this account, and how many times this
# account has been “friended”.

cur.execute('''
CREATE TABLE IF NOT EXISTS Twitter
(name TEXT, retrieved INTEGER, friends INTEGER)''')

# In the main loop of the program, we prompt the user for a Twitter account name
# or “quit” to exit the program. If the user enters a Twitter account, we retrieve the
# list of friends and statuses for that user and add each friend to the database if not
# already in the database. If the friend is already in the list, we add 1 to the friends
# field in the row in the database.

while True:
    acct = input('Enter a Twitter account, or quit: ')
    if ( acct == 'quit' ) : break
    if ( len(acct) < 1 ) :
        cur.execute('SELECT name FROM Twitter WHERE retrieved = 0 LIMIT 1')
    try:
        acct = cur.fetchone()[0]
    except:
        print('No unretrieved Twitter accounts found')
        continue
        
##  For found Twitter Accounts
    url = twurl.augment(TWITTER_URL,
        {'screen_name': acct, 'count': '20'} )
    print('Retrieving', url)
    connection = urllib.urlopen(url)
    data = connection.read()
    headers = connection.info().dict
#   print('Remaining', headers['x-rate-limit-remaining'])
    js = json.loads(data)
    print(json.dumps(js, indent=4))

##  Add Twitter data to database
    #cur.execute('UPDATE Twitter SET retrieved=1 WHERE name = ?', (acct, ))
    cur.execute('UPDATE Twitter SET retrieved=1 WHERE name = ?', (acct, ))

    
# If the user presses enter, we look in the database for the next Twitter account that
# we have not yet retrieved, retrieve the friends and statuses for that account, add
# them to the database or update them, and increase their friends count.

    countnew = 0
    countold = 0
    
    for u in js['users'] :
        friend = u['screen_name']
        print(friend)
        cur.execute('SELECT friends FROM Twitter WHERE name = ? LIMIT 1',
            (friend, ) )
        try:
            count = cur.fetchone()[0]
            cur.execute('UPDATE Twitter SET friends = ? WHERE name = ?',
                (count+1, friend) )
            countold = countold + 1
        except:
            cur.execute('''INSERT INTO Twitter (name, retrieved, friends)
                VALUES ( ?, 0, 1 )''', ( friend, ) )
            countnew = countnew + 1  

# Once we retrieve the list of friends and statuses, we loop through all of the user
# items in the returned JSON and retrieve the screen_name for each user. Then
# we use the SELECT statement to see if we already have stored this particular
# screen_name in the database and retrieve the friend count (friends) if the record
# exists.            
    print('New accounts=',countnew,' revisited=',countold)
    conn.commit()
cur.close()

countnew = 0
countold = 0
for u in js['users'] :
    friend = u['screen_name']
    print(friend)
    cur.execute('SELECT friends FROM Twitter WHERE name = ? LIMIT 1',
        (friend, ) )
    try:
        count = cur.fetchone()[0]
        cur.execute('UPDATE Twitter SET friends = ? WHERE name = ?',
            (count+1, friend) )
        countold = countold + 1
    except:
        cur.execute('''INSERT INTO Twitter (name, retrieved, friends)
            VALUES ( ?, 0, 1 )''', ( friend, ) )
        countnew = countnew + 1
print('New accounts=',countnew,' revisited=',countold)
conn.commit()

# Once the cursor executes the SELECT statement, we must retrieve the rows. We
# could do this with a for statement, but since we are only retrieving one row
# (LIMIT 1), we can use the fetchone() method to fetch the first (and only) row
# that is the result of the SELECT operation. Since fetchone() returns the row
# as a tuple (even though there is only one field), we take the first value from
# the tuple using [0] to get the current friend count into the variable count.

# If this retrieval is successful, we use the SQL UPDATE statement with a WHERE
# clause to add 1 to the friends column for the row that matches the friend’s ac-
# count. Notice that there are two placeholders (i.e., question marks) in the SQL,
# and the second parameter to the execute() is a two-element tuple that holds the
# values to be substituted into the SQL in place of the question marks.

# If the code in the try block fails, it is probably because no record matched the
# WHERE name = ? clause on the SELECT statement. So in the except block, we
# use the SQL INSERT statement to add the friend’s screen_name to the table with
# an indication that we have not yet retrieved the screen_name and set the friend
# count to zero.