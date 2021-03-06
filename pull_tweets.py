import csv
import json
from pymongo import MongoClient
import requests
from requests_oauthlib import OAuth1
import cnfg
import time
from random import randint
import os
import inspect, os

cwd = inspect.getfile(inspect.currentframe())

file_name = 'pull_tweets.py'

cwd = cwd[0:len(cwd)-len(file_name)]

config = cnfg.load(".twitter_develop")
#cwd = os.getcwd()

oauth = OAuth1(config["consumer_key"],
               config["consumer_secret"],
               config["access_token"],
               config["access_token_secret"])

twitter_dict = {}
with open(cwd + 'twitter_accounts.csv', 'rb') as csvfile:
    twitter_accounts = csv.reader(csvfile, delimiter=',')
    for row in twitter_accounts:
        twitter_dict[row[0]] = [row[1], row[2]]
        
client = MongoClient()
db = client.basketball
tweets = db.tweets

updated_fields = []

for name, name_info in twitter_dict.items():
    screen_name = name_info[0]
    response = requests.get("https://api.twitter.com/1.1/statuses/user_timeline.json?count=200&screen_name="+screen_name,auth=oauth)
    while response.status_code != 200:
        print "Waiting for webpage to respond"
        print screen_name
        time.sleep(randint(1,10))
        response = requests.get("https://api.twitter.com/1.1/statuses/user_timeline.json?count=200&screen_name="+screen_name,auth=oauth)
    tweet = response.json()
    for status in tweet:
        if tweets.find_one({'id_str':status['id_str']}) == None:
            tweets.insert(status)
            updated_fields.append(status['id_str'])
        else:
            print "Tweet exists in Database: " + status['id_str']

print "Completed Updating Database"

with open(cwd + "pull_tweets_log.txt", "a") as myfile:
    myfile.write("Number of tweets added on " + time.asctime() + ":")
    myfile.write("\n")
    myfile.write(str(len(updated_fields)) + " Tweets")
    myfile.write("\n")









