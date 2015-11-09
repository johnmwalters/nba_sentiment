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

file_name = 'pull_game_stats.py'

cwd = cwd[0:len(cwd)-len(file_name)]

twitter_dict = {}
with open(cwd + 'twitter_accounts.csv', 'rb') as csvfile:
    twitter_accounts = csv.reader(csvfile, delimiter=',')
    for row in twitter_accounts:
        twitter_dict[row[0]] = [row[1], row[2]]

client = MongoClient()
db = client.basketball
game_logs = db.game_logs

updated_fields = []

for name, name_info in twitter_dict.items():
    nba_id = name_info[1]
    response = requests.get("http://stats.nba.com/stats/playergamelog?LeagueID=00&PlayerID=" + nba_id + "&Season=2015-16&SeasonType=Regular+Season")
    while response.status_code != 200:
        print "Waiting for webpage to respond"
        print "203457"
        time.sleep(randint(1,10))
        response = requests.get("http://stats.nba.com/stats/playergamelog?LeagueID=00&PlayerID=" + nba_id + "&Season=2015-16&SeasonType=Regular+Season")
    game_log_json = response.json()
    if len(game_log_json['resultSets'][0]['rowSet']) != 0:
        for stats in game_log_json['resultSets'][0]['rowSet']:
            game_log = {}
            game_log = dict(zip(game_log_json['resultSets'][0]['headers'], stats))
            if game_logs.find_one({'Player_ID': game_log['Player_ID'], 'Game_ID': game_log['Game_ID'], 'SEASON_ID': game_log['SEASON_ID']}) == None:
                game_logs.insert(game_log)
                updated_fields.append(str(game_log['SEASON_ID']) + " " + str(game_log['Game_ID']) + " " +  str(game_log['Player_ID']))
            else:
                print "Unique Game Log exists in Database: " + str(game_log['SEASON_ID']) + " " + str(game_log['Game_ID']) + " " +  str(game_log['Player_ID'])
    else:
        print "No data for player"

print "Completed Updating Database"

with open(cwd + "pull_game_stats_log.txt", "a") as myfile:
    myfile.write("Number of games added on " + time.asctime() + ":")
    myfile.write("\n")
    myfile.write(str(len(updated_fields)) + " Games")
    myfile.write("\n")