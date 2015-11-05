import csv
import json
from pymongo import MongoClient
import requests
from requests_oauthlib import OAuth1
import cnfg
import time
from random import randint
import os

config = cnfg.load(".twitter_develop")
cwd = os.getcwd()

oauth = OAuth1(config["consumer_key"],
               config["consumer_secret"],
               config["access_token"],
               config["access_token_secret"])

twitter_dict = {}
with open(cwd + '/twitter_accounts.csv', 'rb') as csvfile:
    twitter_accounts = csv.reader(csvfile, delimiter=',')
    for row in twitter_accounts:
        twitter_dict[row[0]] = row[1]
        
client = MongoClient()
db = client.basketball
tweets = db.tweets

for name, screen_name in twitter_dict.items():
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
        else:
            print "Tweet exists in Database: " + status['id_str']

print "Completed Updating Database"