import json
from functools import wraps
import os
import nltk
import random
# import json
from subprocess32 import STDOUT, check_output as qx
from collections import Counter
import sqlite3
import nltk
import time
from joblib import Parallel, delayed
import multiprocessing
from multiprocessing import Process, Manager
import csv
import urllib3
import dateutil.parser
from collections import OrderedDict
import datetime

youtubeKey = 'AIzaSyCleMRxeVkRDzBHDGWvUk-gBu7qPLrMCj8'
youtubeGetChannelIdUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&maxResults=2&key=" + youtubeKey + "&q=" #"https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&maxResults=2&key=AIzaSyCg0kT3wqdZfjHz1a1vJzZvdpL28rMiRQA&q="
youtubeGetActivityUrl = "https://www.googleapis.com/youtube/v3/activities?part=snippet%2CcontentDetails&maxResults=5&key=" + youtubeKey + "&channelId=" #AIzaSyBpMTMC61LBMinww2etKXw2CrqwtbWjemI"
youtubeGetVideoStats = "https://www.googleapis.com/youtube/v3/videos?part=statistics" + "&key=" + youtubeKey + "&id="
youtubeGetChannelStats = "https://www.googleapis.com/youtube/v3/channels?part=statistics" + "&key=" + youtubeKey + "&id="
# https://www.googleapis.com/youtube/v3/channels?part=statistics&id=UCPvWHfKpR4mb7bfZuzaQM3A&key={YOUR_API_KEY}
flagggingAPIBase = 'http://ec2-23-20-32-78.compute-1.amazonaws.com/CatchAViral/v1/'


def search_song(artist_info , idx):
	'''Searches for given string as youtube channel and gets a channel only if number of subscribers > 1000'''
	# build api url
	artist_name = artist_info[1]
	artist_id = artist_info[0]
	print idx
	# print artist_name

	channelIds = []
	try:
		#parse response body
		url = youtubeGetChannelIdUrl
		url += artist_name.replace(' ','+') ###spaces need to be encoded
		# print url

		#response = json.load( urllib2.urlopen( url ) )
		#print type(urllib2.urlopen( url ))
		#print "\n***Processing " + track_file + "\n"
		# print url
		http = urllib3.PoolManager()
		r = http.request('GET',url)
		#print type(r)
		# print r
		response = json.loads(r.data)
		#print response
		#response = json.load( urllib2.urlopen( url ) )
		# print response
		
		# channelTitle = ''
		for item in response['items']:
			channelIds.append(item['snippet']['channelId'])
		# print channelIds
		# channelTitle = response['items'][0]['snippet']['title']
	except:
		channelIds.append('')

	subscribers = []
	for channelId in channelIds:
		url = youtubeGetChannelStats + channelId
		http = urllib3.PoolManager()
		r = http.request('GET',url)
		#print type(r)
		response = json.loads(r.data)

		# subscribers = 0

		try:
			subscribers.append( int(response['items'][0]['statistics']['subscriberCount']) )
		except:
			subscribers.append(0)
	
	# print subscribers
	combine = zip(subscribers,channelIds)
	combine.sort(reverse = True)

	# print combine

	# print combine[0][1]
	# print artist_name
	try:
		if int(combine[0][0]) > 1000:
			return (artist_id,artist_name, combine[0][1], combine[0][0])
		else:
			return (artist_id,artist_name, '', 0)
	except:
		return (artist_id,artist_name, '', 0)


def read_song_list (in_file = 'movie_song_info_rdio.json' ):
	'''Read artist list and store youtube channel list'''

	out_file = in_file.replace('.json','_with_youtube.json')

	inputs = open(in_file,'rb')
	# csvReadr = csv.reader(inputs, delimiter = '\t')
	# csvReadr.next()

	albums = json.load(inputs)

	new_albums = []

	for album in albums
		tracks = album['track_list']
		yr = dateutil.parser.parse(album['release_date']).year
		new_tracks = []

		for tr in tracks:
			tr['youtube_url'] = search_song(tr['track_name'], yr)
			new_tracks.append(tr)
	#skip header
	# num_cores = multiprocessing.cpu_count() #- 1
	
	# outputDict = {}
	# results = Parallel(n_jobs=num_cores)(delayed(searchArtistName)(row , idx) for idx,row in enumerate(csvReadr))

	# output = open(out_file, 'wb')
	# csvWriter = csv.writer(output, delimiter = '\t')

	# csvWriter.writerow(['artist','link'])
	# return results
	# for channel in results:
	# 	artist_id = channel[0]
	# 	name = channel[1]
	# 	channelId = channel[2]
	# 	subscribers = channel[3]
	# 	channelLink = 'https://www.youtube.com/channel/' + channelId
	# 	if int(subscribers) != 0:
	# 		csvWriter.writerow([artist_id,channelLink, channelId, subscribers])

	# output.close()
	return 'done'

if __name__ == "__main__":
	readArtistList()



