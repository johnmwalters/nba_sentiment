import csv
import json
from pymongo import MongoClient
import requests
from requests_oauthlib import OAuth1
import cnfg
import time
from random import randint
import os
#from datetime import now

import inspect, os

cwd = inspect.getfile(inspect.currentframe())

file_name =  'update_users.py'

cwd = cwd[0:len(cwd)-len(file_name)]

config = cnfg.load(".twitter_develop")
#cwd = os.getcwd()

oauth = OAuth1(config["consumer_key"],
               config["consumer_secret"],
               config["access_token"],
               config["access_token_secret"])

twitter_dict = {}
with open(cwd+'twitter_accounts.csv', 'rb') as csvfile:
    twitter_accounts = csv.reader(csvfile, delimiter=',')
    for row in twitter_accounts:
        twitter_dict[row[0]] = [row[1], row[2]
        
client = MongoClient()
db = client.basketball
users = db.users

updated_fields = []

for name, name_info in twitter_dict.items():
    screen_name = name_info[0]
    if users.find_one({'screen_name':screen_name}) == None:
        updated_fields.append(screen_name)
        response = requests.get("https://api.twitter.com/1.1/users/show.json?screen_name=" + screen_name,auth=oauth)
        while response.status_code != 200:
            print "Waiting for webpage to respond"
            print screen_name
            time.sleep(randint(1,10))
            response = requests.get("https://api.twitter.com/1.1/users/show.json?screen_name=" + screen_name,auth=oauth)
        user = response.json()
        users.insert(user)

if len(updated_fields) == 0:
    updated_fields = ['No fields were updated']

print "Completed Updating Database"

with open(cwd + "update_users_log.txt", "a") as myfile:
    myfile.write("The following users were added on " + time.asctime() + ":")
    myfile.write("\n")
    myfile.write(str(updated_fields))
    myfile.write("\n")







