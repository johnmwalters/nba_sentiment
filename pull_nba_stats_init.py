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
import arrow
import time
from datetime import date
from datetime import timedelta

'''
Determine file path so script can run in cron
'''
cwd = inspect.getfile(inspect.currentframe())
file_name = 'pull_nba_stats.py'
cwd = cwd[0:len(cwd)-len(file_name)]

'''
Define Mongo DB collections for update
'''
client = MongoClient()
db = client.basketball
game_dates = db.game_dates
game_logs = db.game_logs
#yesterday = date.today() - timedelta(days = 1)
#yesterday_str = str(arrow.get(yesterday))[:-6]

def date_json(date):
	'''
	Search NBA API for json file containing games that happened on specific date
	'''
	day = date.day
	month = date.month
	year = date.year
	date = str(month) + "%2F" + str(day) + "%2F" + str(year)
	response = requests.get("http://stats.nba.com/stats/scoreboardV2?DayOffset=0&LeagueID=00&gameDate=" + date)
	while response.status_code != 200:
	    print "Waiting for webpage to respond"
	    print date
	    time.sleep(randint(1,10))
	    response = requests.get("http://stats.nba.com/scores/#!/" + date)
	game_date_json = response.json()
	return game_date_json

def date_mongo_update(game_date_json):
	'''
	Update Mongo DB with games that happened on previously specified date
	'''
	for stats in game_date_json['resultSets'][0]['rowSet']:
	    game_date = {}
	    game_date = dict(zip(game_date_json['resultSets'][0]['headers'], stats))
	    if game_dates.find_one({'GAME_ID': game_date['GAME_ID']}) == None:
	        game_dates.insert(game_date)
	    else:
	        print "Unique Game exists in Database: " + str(game_date['GAME_ID'])

def game_mongo_update(date_string):
	for doc in game_dates.find({'GAME_DATE_EST': date_string},{'GAME_ID':1}):
	    game_id = doc['GAME_ID']
	    response = requests.get("http://stats.nba.com/stats/boxscoretraditionalv2?EndPeriod=10&EndRange=28800&RangeType=0&StartPeriod=1&StartRange=0&GameID=" + game_id)
	    while response.status_code != 200:
	        print "Waiting for webpage to respond"
	        print game_id
	        time.sleep(randint(1,10))
	        response = requests.get("http://stats.nba.com/stats/boxscoretraditionalv2?EndPeriod=10&EndRange=28800&RangeType=0&StartPeriod=1&StartRange=0&GameID=" + game_id)
	    game_log_json = response.json()
	    if len(game_log_json['resultSets'][0]['rowSet']) != 0:
	        for stats in game_log_json['resultSets'][0]['rowSet']:
	            game_log = {}
	            game_log = dict(zip(game_log_json['resultSets'][0]['headers'], stats))
	            if game_logs.find_one({'PLAYER_ID': game_log['PLAYER_ID'], 'GAME_ID': game_log['GAME_ID']}) == None:
	                game_logs.insert(game_log)
	                updated_fields.append(str(game_log['GAME_ID']) + " " +  str(game_log['PLAYER_ID']))
	            else:
	                print "Unique Game Log exists in Database: " + str(game_log['GAME_ID']) + " " +  str(game_log['PLAYER_ID'])
	    else:
	        print "No data for player"

for x in range(1, 16):
	yesterday = date.today() - timedelta(days = x)
	yesterday_str = str(arrow.get(yesterday))[:-6]
	print yesterday_str
	game_date_json = date_json(yesterday)
	date_mongo_update(game_date_json)
	updated_fields = []
	game_mongo_update(yesterday_str)

print "Completed Updating Database"

with open(cwd + "pull_nba_stats_log_init.txt", "a") as myfile:
    myfile.write("Number of games added on " + time.asctime() + ":")
    myfile.write("\n")
    myfile.write(str(len(updated_fields)) + " Games")
    myfile.write("\n")