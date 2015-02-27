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
# from twisted.internet import defer

from pprint import pformat

from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol
from twisted.web.client import Agent,HTTPConnectionPool
from twisted.web.http_headers import Headers
from twisted.internet import defer, reactor, task
from twisted.internet.defer import inlineCallbacks, returnValue

youtubeKey = 'AIzaSyCleMRxeVkRDzBHDGWvUk-gBu7qPLrMCj8' 
youtubeGetChannelIdUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&maxResults=2&key=" + youtubeKey + "&q=" #"https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&maxResults=2&key=AIzaSyCg0kT3wqdZfjHz1a1vJzZvdpL28rMiRQA&q="
youtubeGetActivityUrl = "https://www.googleapis.com/youtube/v3/activities?part=snippet%2CcontentDetails&maxResults=5&key=" + youtubeKey + "&channelId=" #AIzaSyBpMTMC61LBMinww2etKXw2CrqwtbWjemI"
youtubeGetVideoStats = "https://www.googleapis.com/youtube/v3/videos?part=statistics" + "&key=" + youtubeKey + "&id="
youtubeGetChannelStats = "https://www.googleapis.com/youtube/v3/channels?part=statistics" + "&key=" + youtubeKey + "&id="
# https://www.googleapis.com/youtube/v3/channels?part=statistics&id=UCPvWHfKpR4mb7bfZuzaQM3A&key={YOUR_API_KEY}
flagggingAPIBase = 'http://ec2-23-20-32-78.compute-1.amazonaws.com/CatchAViral/v1/'

class format_response(Protocol):
    def __init__(self, finished):
        self.finished = finished
        self.remaining = 1024 * 10
        self.output = ''

    def dataReceived(self, bytes):
        if self.remaining:
            display = bytes[:self.remaining]
            # print 'Some data received:'
            # print '1'
            # print display
            self.output  += display
            self.remaining -= len(display)
        # self.output  = display
        # self.finished.callback(self.output)


    def connectionLost(self, reason):
        # print 'Finished receiving body:', reason.getErrorMessage()
        self.finished.callback(self.output)

def make_request(response):
    # print 'Response version:', response.version
    # print 'Response code:', response.code
    # print 'Response phrase:', response.phrase
    # print 'Response headers:'
    # print pformat(list(response.headers.getAllRawHeaders()))
    # return response.deliverBody()
    finished = Deferred()
    response.deliverBody(format_response(finished))
    return finished

def send_json(resp):
	# k = 
	# print k['message']['body']['track']['track_edit_url']
	response = json.loads(resp)
	# print response
	f = Deferred()
	f.callback(response)
	return f

def compare_subscribers(response, artist_name,artist_id):
	# print artist_id
	subscribers = []
	channel_ids = []
	for channel_info in response['items']:
		channel_ids.append(channel_info['id'])
		subscribers.append(int(channel_info['statistics']['subscriberCount']))

	# print artist_name
	# print subscribers
	# print channel_ids
	# print subscribers
	combine = zip(subscribers,channel_ids)
	combine.sort(reverse = True)

	try:
		if int(combine[0][0]) > 1000:
			result =  (artist_id,artist_name, combine[0][1], combine[0][0])
		else:
			result = (artist_id,artist_name, '', 0)
	except:
		result = (artist_id,artist_name, '', 0)

	return defer.succeed(result)


@defer.inlineCallbacks
def get_channel_subscribers(response,artist_name,artist_id):
	channelIds = []

	try:
		for item in response['items']:
			channelIds.append(item['snippet']['channelId'])
	except:
		channelIds.append('')

	subscribers = []
	deferred_list = []
	url = youtubeGetChannelStats
	for channelId in channelIds:
		url += channelId + ','
		# deferred_list.append(get_url(url))

	# dl = defer.DeferredList(deferred_list, consumeErrors=True)
	# dl.addCallback(write_result)
	# subscribers = 0
	try:
	# url
		result = yield get_url(url).addCallback(compare_subscribers, artist_name,artist_id)
	except Exception as err:
		print err
		result = (artist_id,artist_name, '', 0)

	returnValue(result)

pool = HTTPConnectionPool(reactor)
agent = Agent(reactor, pool=pool)


def get_url(url):
	url = str(url)
	# print url
	# print type(url)
	# agent = Agent(reactor)
	d = agent.request('GET',url,Headers({'User-Agent': ['Twisted Web Client Example']}),None)
	d.addCallback(make_request)
	d.addCallback(send_json)
	return d
	# reactor.stop()
# d.addBoth(cbShutdown)

def shut_all(ignored):
	# print 'herereerererer'
	reactor.stop()

@defer.inlineCallbacks
def search_artist(artist_info = [123,'Nirvana'] , idx = 1):
	artist_name = artist_info[1]
	artist_id = artist_info[0]
	url = youtubeGetChannelIdUrl
	url += artist_name.replace(' ','+')
	# print artist_name
	# url = 
	try:
		result = yield get_url(url).addCallback(get_channel_subscribers, artist_name,artist_id)
		# print url
		# print idx
		# print result
		returnValue(result)
	except Exception as err:
		# print url
		# print idx
		# print artist_info
		print "Error", err
	# finally:
	# 	print "Shutting down"
	# 	reactor.stop()
	# j.addBoth(shut_all)
	# reactor.run()




def save_to_file(result, out_file):
	results = []
	for (success, value) in result:
		if success:
            # print 'Success:', value
			results.append(value)
		else:
			print 'Failure:', value.getErrorMessage()
 	output = open(out_file, 'ab')
	csvWriter = csv.writer(output, delimiter = '\t')

	# csvWriter.writerow(['artist','link'])
	# return results
	try:
		for channel in results:
			artist_id = channel[0]
			name = channel[1]
			channelId = channel[2]
			subscribers = channel[3]
			channelLink = 'https://www.youtube.com/channel/' + channelId
			if int(subscribers) != 0:
				csvWriter.writerow([artist_id,channelLink, channelId, subscribers])
	except:
		pass
	output.close()
	return defer.succeed(result)

lines = 0

def repeat_process_chunk(resp, csvReadr,limit_connections, out_file):
	process_chunk(csvReadr, limit_connections, out_file)
	defer.succeed('success')

def process_chunk(csvReadr,limit_connections = 3000, out_file = 'asd_output.txt'):
	deferred_list = []
	# print limit_connections
	# results = Parallel(n_jobs=num_cores)(delayed(searchArtistName)(row , idx) for idx,row in enumerate(csvReadr))
	for idx,row in enumerate(csvReadr):
		# d = defList.append(searchArtistName(row,idx))
		# d.addCallback(handleResult)
		# print idx
		if idx == limit_connections:
			break
		# val = yield
		
		# print lines
		deferred_list.append(search_artist(row,idx))
	
	global lines
	lines = lines - limit_connections	
	dl = defer.DeferredList(deferred_list)
	dl.addCallback(save_to_file,out_file)
	print lines
	if lines <= 0:
		
		dl.addBoth(shut_all)
	else:
		dl.addCallback(repeat_process_chunk, csvReadr,limit_connections, out_file )



def readArtistList (in_file = 'allCountryArtists.txt' , out_file = 'asd_output.txt'):
	'''Read artist list and store youtube channel list'''

	inputs = open(in_file,'rb')
	inp =open(in_file,'rb')
	output = open(out_file, 'wb')
	output.close()
	csvReadr = csv.reader(inputs, delimiter = '\t')
	csvReadr.next() 
	#skip header
	input_lines = len(inp.readlines())
	global lines
	lines = input_lines
	print lines
	countr = 0
	# while countr < input_lines:
		# from twisted.internet import defer, reactor
		# print reactor.running
		# if reactor.running:
		# 	print 'asd'
		# 	reactor.stop()
	process_chunk(csvReadr, 500, out_file)
	# print 'asd'
	# if not reactor.running:
	# t.start(10)
	reactor.run()
	# num_cores = multiprocessing.cpu_count() #- 1
	# reactor.stop()
	outputDict = {}
	results = []
	

		# print val
		# results.append(val)
	# finally:
	# 	print "Shutting down"
	# 	reactor.stop()
	# 	# print results
	# 	output = open(out_file, 'wb')
	# 	csvWriter = csv.writer(output, delimiter = '\t')

	# 	# csvWriter.writerow(['artist','link'])
	# 	# return results
	# 	for channel in results:
	# 		artist_id = channel[0]
	# 		name = channel[1]
	# 		channelId = channel[2]
	# 		subscribers = channel[3]
	# 		channelLink = 'https://www.youtube.com/channel/' + channelId
	# 		if int(subscribers) != 0:
	# 			csvWriter.writerow([artist_id,channelLink, channelId, subscribers])

	# 	output.close()
		# return 'done'
		# returnValue(results)


	



	# output = open(out_file, 'wb')
	# csvWriter = csv.writer(output, delimiter = '\t')

	# # csvWriter.writerow(['artist','link'])
	# # return results
	# for channel in results:
	# 	artist_id = channel[0]
	# 	name = channel[1]
	# 	channelId = channel[2]
	# 	subscribers = channel[3]
	# 	channelLink = 'https://www.youtube.com/channel/' + channelId
	# 	if int(subscribers) != 0:
	# 		csvWriter.writerow([artist_id,channelLink, channelId, subscribers])

	# output.close()
	# return 'done'

def read_song_list (in_file = 'movie_song_info_rdio.json' ):
	'''Read artist list and store youtube channel list'''

	out_file = in_file.replace('.json','_with_youtube.json')

	inputs = open(in_file,'rb')
	# csvReadr = csv.reader(inputs, delimiter = '\t')
	# csvReadr.next()

	albums = json.load(inputs)

	new_albums = []

	for album in albums:
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



def searchArtistName(artist_info , idx):
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
			result =  (artist_id,artist_name, combine[0][1], combine[0][0])
		else:
			result = (artist_id,artist_name, '', 0)
	except:
		result = (artist_id,artist_name, '', 0)

	return defer.succeed(result)

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
			result =  (artist_id,artist_name, combine[0][1], combine[0][0])
		else:
			result = (artist_id,artist_name, '', 0)
	except:
		result = (artist_id,artist_name, '', 0)

	return defer.succeed(result)

# def deferredExample():
#     d = defer.Deferred()
#     d.addCallback(handleResult)
#     d.addErrback(handleFailure)

#     d.callback("success")
# @defer.inlineCallbacks