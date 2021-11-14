#import os for the env file stuff
import os
from datetime import datetime, timedelta
import json

#import discord package
import discord
from discord.ext import tasks
from dotenv import load_dotenv
import boto3
import fandom
import sqlite3
from sqlite3 import Error

#load the .env file and fetch the discord token from it
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

#Client (our bot)
intents = discord.Intents.default()
intents.members = True
intents.presences = True
client = discord.Client(intents=intents)

#Amazon EC2 instance
ec2 = boto3.client('ec2')
cloudwatch = boto3.client('cloudwatch')

#Event called when the bot is started
@client.event
async def on_ready():
  #Start the CloudMetrics loop
  check_server_status.start()
  instantiateDatabase()

#Event called when a new message is sent by a user
@client.event
async def on_message(message):
  if message.author == client.user:
    return

  if message.content.startswith('!StartServer'):
    await message.channel.send('Starting Minecraft server')
    ec2.start_instances(InstanceIds=[os.getenv('MinecraftInstance')], DryRun=False)

  if message.content.startswith('!StopServer'):
    await message.channel.send('Stopping Minecraft server')
    ec2.stop_instances(InstanceIds=[os.getenv('MinecraftInstance')], DryRun=False)

  if message.content.startswith('!Wiki'):
      searchTerm = message.content.split("!Wiki",1)[1] #splits the string to return what's after !Wiki
      searchResults = searchWiki(searchTerm)
      await message.channel.send(searchResults[0].url, embed=searchResults[1])

  if message.content.startswith('!SQLStats'):
        await message.channel.send('Pulling list of games for user and displaying in order of time')
        result = pullUserStatisticsDatabase(message.author)
        embed = discord.Embed(title="Statistics")
        for line in result:
            beginTime = line[3]
            endTime = line[4]
            if beginTime == '' or endTime == '':
                break

            timediff = datetime.strptime((line[4].split(".")[0]), '%Y-%m-%d %H:%M:%S') - datetime.strptime((line[3].split(".")[0]), '%Y-%m-%d %H:%M:%S')
            embed.add_field(name=line[2], value="Time: " + str(timediff), inline=False)
            
        await message.channel.send(embed=embed)

@client.event
async def on_member_update(before:discord.Member, after:discord.Member):
    afterActivity = None

    if after.activity == None:
        afterActivity = "None"
    else:
        afterActivity = after.activity.name

    cur = con.cursor()

    #Add the new activity in the database if it doesn't exits
    cur.execute("INSERT OR IGNORE INTO activities (activity_name) VALUES (?)", (str(afterActivity),))
    #Add an ending timestamp for the user's previous activity
    cur.execute("UPDATE OR IGNORE statistics SET end_time = ? WHERE userid = ? AND end_time = ''", (datetime.now(), after.id,))
    #Add a new entry with a starting timestamp for the new activity
    cur.execute("INSERT INTO statistics (userid, activity_name, start_time, end_time) VALUES (?,?,?,?)", (after.id, str(afterActivity), datetime.now(), '',))
    #Update the member's nickname if it's different
    addNewUserDatabase(after)
    cur.execute("UPDATE users SET username = ? WHERE userid = ?", (str(after.display_name), after.id,))

    con.commit()

#Event called every 5 minutes
@tasks.loop(seconds=300)
async def check_server_status():
  #Storing the metric results in this string to print
  resultsStr = 'Results: '

  #First check if the instance is up
  response = ec2.describe_instance_status(InstanceIds=[os.getenv('MinecraftInstance')])
  if len(response['InstanceStatuses']) > 0 and response['InstanceStatuses'][0]['InstanceState']['Name'] == 'running':
    result = cloudwatch.get_metric_data(
      MetricDataQueries=[
        {
          'Id': 'm1',
          'MetricStat': {
            'Metric': {
              'Namespace':'AWS/EC2',
              'MetricName': 'NetworkOut',
              'Dimensions': [
                {
                  'Name': 'InstanceId',
                  'Value': os.getenv('MinecraftInstance')
                },
              ]
            },
            'Period': 300,
            'Stat': 'Average',
            'Unit': 'Bytes'
          },
        },
      ],
      StartTime= (datetime.now() - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S.676Z'), #Locally, I have to adjust for UTC since I'm in -08:00.  + timedelta(hours=6)
      EndTime= (datetime.now()).strftime('%Y-%m-%dT%H:%M:%S.676Z'), #Locally, I have to adjust for UTC since I'm in -08:00.  + timedelta(hours=8)
      LabelOptions={
      }
    )

    jsonStr = json.dumps(result, indent=4, sort_keys=True, default=str)
    jsonData = json.loads(jsonStr)
    if len(jsonData['MetricDataResults'][0]['Values']) > 6: # I want at least 6 reports of traffic (30 minutes), otherwise it will shut down the server on startup
      shutdownServer = True #start with True, flip to False if you find high network traffic (which means it would only shut down if the last 30 minutes of traffic are low)
      index = 0 #index to keep track of how many values we're looking at, and only process the last 30 minutes
      for i in jsonData['MetricDataResults'][0]['Values']:  
        index += 1
        if index > 6: #If we're looking beyond 30 minutes, then stop the loop and decide to shut down or not
          break
        elif i > 2000:
          shutdownServer = False
        
        #adding this value to the string so that I can print it to the discord channel at the end 
        resultsStr += str(i) + ', '

      if shutdownServer:
        ec2.stop_instances(InstanceIds=[os.getenv('MinecraftInstance')], DryRun=False)
        await client.get_channel(os.getenv('DiscordGeneralChannel')).send('Shutting server down due to inactivity')


def searchWiki(searchTerm):
    if searchTerm != "":
        result = fandom.search(searchTerm, results = 1)
        page = fandom.page(pageid = result[0][1]) #multi-dimensional array because the first array is the result, and second is the result details
        e = discord.Embed()
        try:
            e.set_thumbnail(url=page.images[0])
        except:
            pass

        e.set_footer()

        e.add_field(name='Summary', value=page.summary, inline=False)
        textValue = page.section(page.sections[0])[:700]+'...' if len(page.section(page.sections[0])) > 700 else page.section(page.sections[0])
        e.add_field(name=page.sections[0], value=textValue, inline=False)

        return page, e


def create_connection(path):
    connection = None
    try:
        connection = sqlite3.connect(path)

    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

def instantiateDatabase():
    cur = con.cursor()
    cur.execute("CREATE TABLE if not exists users(userid integer primary key, username text, role text)")
    cur.execute("CREATE TABLE if not exists activities(activity_name text primary key)")
    cur.execute("CREATE TABLE if not exists statistics(statistic_id integer primary key, userid integer, activity_name text, start_time timestamp, end_time timestamp)")
    
    for guild in client.guilds:
        for member in guild.members:
            cur.execute("INSERT OR IGNORE INTO users (userid, username, role) VALUES (?,?,?)", (member.id, str(member.display_name), str(member.top_role),))
            #should replace the or ignore with a check for the row, and replacing the role / name if needed
    
    con.commit()

def addNewUserDatabase(member:discord.member):
    cur = con.cursor()
    cur.execute("CREATE TABLE if not exists users(userid integer primary key, username text, role text)")
    cur.execute("INSERT OR IGNORE INTO users (userid, username, role) VALUES (?,?,?)", (member.id, str(member.display_name), str(member.top_role,)))
    
    con.commit()

def pullUserStatisticsDatabase(member:discord.member):
    cur = con.cursor()
    cur.execute("SELECT * FROM statistics WHERE userid=? ORDER BY activity_name ASC",(member.id,))
    result = cur.fetchall()

    listFormat = []
    i = 0 #represents the activity
    for line in result:
        if i == 0:
            listFormat.append(line)
            i += 1
        elif line[2] == listFormat[i-1][2]:
            #If it's the same name, check if the end time is bigger and the start time is smaller. Overwrite
            prevStartTime:datetime = datetime.strptime((listFormat[i-1][3].split(".")[0]), '%Y-%m-%d %H:%M:%S')
            newStartTime:datetime = datetime.strptime((line[3].split(".")[0]), '%Y-%m-%d %H:%M:%S')
            if newStartTime - prevStartTime < timedelta(minutes=0):
                newline = (listFormat[i-1][0], listFormat[i-1][1], listFormat[i-1][2], str(newStartTime), listFormat[i-1][4])
                listFormat[i-1] = newline

            prevEndTime:datetime = datetime.strptime((listFormat[i-1][4].split(".")[0]), '%Y-%m-%d %H:%M:%S')
            if line[4] != '':
                newEndTime:datetime = datetime.strptime((line[4].split(".")[0]), '%Y-%m-%d %H:%M:%S')
                if newEndTime - prevEndTime > timedelta(minutes=0):
                    newline = (listFormat[i-1][0], listFormat[i-1][1], listFormat[i-1][2], listFormat[i-1][3], str(newStartTime))
                    listFormat[i-1] = newline 
        elif line[2] != 'None':
            #If it's not the same and also not "none", append 
            listFormat.append(line)
            i += 1

    return listFormat

#Run the client on the server and start the database
fandom.set_wiki('minecraft')
con = create_connection('database.sqlite')
client.run(TOKEN)