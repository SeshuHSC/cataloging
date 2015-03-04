from simple_requests import Requests
import csv
from joblib import Parallel, delayed
import multiprocessing
from multiprocessing import Process, Manager
import urllib
import json
import dateutil.parser

requests = Requests()


youtubeKey = 'AIzaSyCleMRxeVkRDzBHDGWvUk-gBu7qPLrMCj8'
youtube_get_video_duration = "https://www.googleapis.com/youtube/v3/videos?part=contentDetails" + "&key=" + youtubeKey + "&id="
# https://www.googleapis.com/youtube/v3/videos?part=contentDetails&key=AIzaSyCleMRxeVkRDzBHDGWvUk-gBu7qPLrMCj8&id=npdTmxuMQZ8
youtube_get_video = "https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&maxResults=5&key=" + youtubeKey + "&q="
# youtubeGetChannelIdUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&maxResults=2&key=" + youtubeKey + "&q=" #"https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&maxResults=2&key=AIzaSyCg0kT3wqdZfjHz1a1vJzZvdpL28rMiRQA&q="
youtube_get_playlist = "https://www.googleapis.com/youtube/v3/search?part=snippet&maxResults=1&type=playlist&key=" + youtubeKey + "&q="
# https://www.googleapis.com/youtube/v3/search?part=snippet&q=lagaan+all+songs+2001&type=playlist&key=AIzaSyCleMRxeVkRDzBHDGWvUk-gBu7qPLrMCj8
# youtubeGetActivityUrl = "https://www.googleapis.com/youtube/v3/activities?part=snippet%2CcontentDetails&maxResults=5&key=" + youtubeKey + "&channelId=" #AIzaSyBpMTMC61LBMinww2etKXw2CrqwtbWjemI"
# youtubeGetVideoStats = "https://www.googleapis.com/youtube/v3/videos?part=statistics" + "&key=" + youtubeKey + "&id="
# youtubeGetChannelStats = "https://www.googleapis.com/youtube/v3/channels?part=statistics" + "&key=" + youtubeKey + "&id="

#https://www.googleapis.com/youtube/v3/videos?part=contentDetails&key=AIzaSyCleMRxeVkRDzBHDGWvUk-gBu7qPLrMCj8&id=a-s3H9Mawm0
def url_constructor(in_file = 'consolidated_song_names.json', search_type = 1):
	""" Construct search urls for tracks. 
		search_type = 1 : Search by track_name
	"""
	input_pointer = open(in_file,'rb')
	movie_song_names = json.load(input_pointer)

	url_list = []
	# artist_ids = []
	key_list = [] #store reverse search info
	
	if search_type == 1:
		for idx,key in enumerate(movie_song_names.keys()):
			# print key
			for idx2,track in enumerate(movie_song_names[key]['track_list']):
				# url = youtubeGetChannelIdUrl
				# print rw[1].decode('utf-8')
				# print rw[1]
				# print track['track_name']
				
				# track_index_list.append()
				if track['track_name'] != None:
					url = youtube_get_video + urllib.quote_plus(track['track_name'].encode('utf8'))
					movie_song_names[key]['track_list'][idx2]['search_type_'+ str(search_type)] = {}
					movie_song_names[key]['track_list'][idx2]['search_type_'+ str(search_type)]['url'] = [url]
					url_list.append(url)
					key_list.append((key,idx2))

				else:
					movie_song_names[key]['track_list'][idx2]['search_type_'+ str(search_type)] = {}
					movie_song_names[key]['track_list'][idx2]['search_type_'+ str(search_type)]['url'] = ['']
				# artist_ids.append(rw[0])

	return (url_list, key_list, movie_song_names)

def url_constructor_videoIds(in_file = 'consolidated_song_names_with_videoIds.json', search_type = 1):
	""" Construct search urls for tracks. 
		search_type = 1 : Search by track_name
	"""
	input_pointer = open(in_file,'rb')
	movie_song_names = json.load(input_pointer)

	url_list = []
	# artist_ids = []
	key_list = [] #store reverse search info
	
	if search_type == 1:
		for idx,key in enumerate(movie_song_names.keys()):
			# print key
			for idx2,track in enumerate(movie_song_names[key]['track_list']):
				# url = youtubeGetChannelIdUrl
				# print rw[1].decode('utf-8')
				# print rw[1]
				# print track['track_name']
				
				# track_index_list.append()
				url = youtube_get_video_duration
				if track['track_name'] != None:
					# print key
					# print track['track_name']
					try:
						for vid in track['search_type_'+ str(search_type)]['video_ids']:
							url += vid + ','
						# movie_song_names[key]['track_list'][idx2]['search_type_'+ str(search_type)] = {}
						# movie_song_names[key]['track_list'][idx2]['search_type_'+ str(search_type)]['url'] = [url]
						url_list.append(url)
						key_list.append((key,idx2))
					except:
						pass

				# else:
					# url_list.append
					# movie_song_names[key]['track_list'][idx2]['search_type_'+ str(search_type)] = {}
					# movie_song_names[key]['track_list'][idx2]['search_type_'+ str(search_type)]['url'] = ['']
				# artist_ids.append(rw[0])
	return (url_list, key_list, movie_song_names)


def url_constructor_playlist_search(in_file = 'consolidated_song_names.json', search_type = 2):
	""" Construct search urls for tracks. 
		search_type = 1 : Search by track_name
	"""
	input_pointer = open(in_file,'rb')

	movie_song_names = json.load(input_pointer)

	url_list = []
	# artist_ids = []
	key_list = [] #store reverse search info
	
	if search_type == 2:
		for idx,key in enumerate(movie_song_names.keys()):
			# print key
			# print movie_song_names[key]['release_date']
			# if movie_song_names[key]['release_date'] != '0000-00-00':
			# 	yr = (dateutil.parser.parse(movie_song_names[key]['release_date'])).year
			# else:
			# 	yr = ''
			yr = movie_song_names[key]['release_date'].split('-')[0]
			if yr == '0000':
				yr = ''
			print yr
			url = youtube_get_playlist + urllib.quote_plus(movie_song_names[key]['album_name'].encode('utf8') +' '+ str(yr)+ ' all songs')
			movie_song_names[key]['search_type_'+ str(search_type)] = {'url': url}
			key_list.append(key)
			url_list.append(url)

			# for idx2,track in enumerate(movie_song_names[key]['track_list']):
			# 	# url = youtubeGetChannelIdUrl
			# 	# print rw[1].decode('utf-8')
			# 	# print rw[1]
			# 	# print track['track_name']
				
			# 	# track_index_list.append()
				
			# 	# yr 
			# 	if track['track_name'] != None:
			# 		# print key
			# 		# print track['track_name']
			# 		try:
			# 			for vid in track['search_type_'+ str(search_type)]['video_ids']:
			# 				url += vid + ','
			# 			# movie_song_names[key]['track_list'][idx2]['search_type_'+ str(search_type)] = {}
			# 			# movie_song_names[key]['track_list'][idx2]['search_type_'+ str(search_type)]['url'] = [url]
			# 			url_list.append(url)
			# 			key_list.append((key,idx2))
			# 		except:
			# 			pass

			# 	# else:
			# 		# url_list.append
			# 		# movie_song_names[key]['track_list'][idx2]['search_type_'+ str(search_type)] = {}
			# 		# movie_song_names[key]['track_list'][idx2]['search_type_'+ str(search_type)]['url'] = ['']
				# artist_ids.append(rw[0])
	return (url_list, key_list, movie_song_names)



def list_chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def async_get(url_list,idx):
	""" Make async get requests to a list of urls and returns the responses and the urls( which might not be in the original order as the input).
	"""
	responses = []
	jumbled_urls = []
	print idx*len(url_list)
	# print idx
	# print len()
	# print url_list
	# try:
	for response in requests.swarm(url_list, maintainOrder = False):
		# print response.url
		responses.append(response)
		jumbled_urls.append(response.url)
	# except:
	# 	print url_list
		# except:
			# continue
		# print response.url

	return responses, jumbled_urls


def search_videos(in_file = 'consolidated_song_names.json', chunk_size = 800, search_type = 1 ):
	""" Search youtube for artist names and select channel with most subscribers only if it has more than min_subscribers 
	"""
	(url_list,key_list,movie_song_names) = url_constructor(in_file,1)

	tracks_info = zip(url_list,key_list)

	tracks_info_chunks = list(list_chunks(tracks_info,chunk_size))
	# return artists_info_lists
	url_chunks = []
	key_chunks = []
	for each in tracks_info_chunks:
		(a,b) = zip(*each)
		url_chunks.append(a)
		key_chunks.append(b)
	print len(url_chunks)

	num_cores = multiprocessing.cpu_count() #- 1
	# print num_cores
	
	outputDict = {}
	results = Parallel(n_jobs= (num_cores ) )(delayed(async_get)(urls , idx) for idx,urls in enumerate(url_chunks))



	jumbled_url_chunks = []
	response_chunks = []

	for each in results:
		jumbled_url_chunks.append(each[1])
		response_chunks.append(each[0])

	total_video_ids = []
	errs = []
	for idx,jumbled_urls in enumerate(jumbled_url_chunks):
		for idx2, q_url in enumerate(jumbled_urls):
			video_ids = []
			video_infos = []

			try:
				loc = url_chunks[idx].index(q_url)
				(movie_name,track_index) = key_chunks[idx][loc]

				resp = response_chunks[idx][idx2].json()
				
				# print resp['items'][0]['id']

				for itm in resp['items']:
					# print itm
					# print type(itm)
					# total_video_ids.append(itm['id']['videoId'])
					video_ids.append(itm['id']['videoId'])
					video_infos.append(itm['snippet'])
					# for id_info in itm:
						# print id_info
						# id_info = json.loads(id_info)
						# print type(id_info)
				
				# if len(video_ids) == 0:
					# print movie_name
					# print track_index
					# print movie_song_names[movie_name]['track_list'][track_index]['track_name']
					# print movie_song_names[movie_name]['track_list'][track_index]['search_type_'+ str(search_type)]['url']
					# print q_url
					# errs.append(response_chunks[idx][idx2])
			except:
				pass

			movie_song_names[movie_name]['track_list'][track_index]['search_type_'+ str(search_type)]['video_ids'] = video_ids
			movie_song_names[movie_name]['track_list'][track_index]['search_type_'+ str(search_type)]['video_infos'] = video_infos

	out_file = in_file.replace('.json','_with_videoIds.json')
	out_file_pointer = open(out_file,'wb')
	json.dump(movie_song_names,out_file_pointer)
	return 'done'


def search_videos_durations(in_file = 'consolidated_song_names_with_videoIds.json', chunk_size = 800, search_type = 1 ):
	""" Search youtube for artist names and select channel with most subscribers only if it has more than min_subscribers 
	"""
	(url_list,key_list,movie_song_names) = url_constructor_videoIds(in_file,1)

	tracks_info = zip(url_list,key_list)

	tracks_info_chunks = list(list_chunks(tracks_info,chunk_size))
	# return artists_info_lists
	url_chunks = []
	key_chunks = []
	for each in tracks_info_chunks:
		(a,b) = zip(*each)
		url_chunks.append(a)
		key_chunks.append(b)
	print len(url_chunks)

	num_cores = multiprocessing.cpu_count() #- 1
	# print num_cores
	
	outputDict = {}
	results = Parallel(n_jobs= (num_cores ) )(delayed(async_get)(urls , idx) for idx,urls in enumerate(url_chunks))



	jumbled_url_chunks = []
	response_chunks = []

	for each in results:
		jumbled_url_chunks.append(each[1])
		response_chunks.append(each[0])

	# total_video_content_details = []
	errs = []

	for idx,jumbled_urls in enumerate(jumbled_url_chunks):
		for idx2, q_url in enumerate(jumbled_urls):
			video_details = []

			try:
				loc = url_chunks[idx].index(q_url)
				(movie_name,track_index) = key_chunks[idx][loc]

				resp = response_chunks[idx][idx2].json()
				
				# print resp['items'][0]['id']

				for itm in resp['items']:
					# print itm
					# print type(itm)
					# total_video_content_details.append(itm['contentDetails'])
					video_details.append(itm['contentDetails'])
						# for id_info in itm:
							# print id_info
							# id_info = json.loads(id_info)
							# print type(id_info)
					
					# if len(video_ids) == 0:
						# print movie_name
						# print track_index
						# print movie_song_names[movie_name]['track_list'][track_index]['track_name']
						# print movie_song_names[movie_name]['track_list'][track_index]['search_type_'+ str(search_type)]['url']
						# print q_url
						# errs.append(response_chunks[idx][idx2])
			except:
				pass

			movie_song_names[movie_name]['track_list'][track_index]['search_type_'+ str(search_type)]['video_details'] = video_details

	out_file = in_file.replace('.json','_video_details.json')
	out_file_pointer = open(out_file,'wb')
	json.dump(movie_song_names,out_file_pointer)
	return 'done'



def search_playlists(in_file = 'consolidated_song_names.json', chunk_size = 800, search_type = 2 ):
	""" Search youtube for artist names and select channel with most subscribers only if it has more than min_subscribers 
	"""
	(url_list,key_list,movie_song_names) = url_constructor_playlist_search(in_file,search_type)

	tracks_info = zip(url_list,key_list)

	tracks_info_chunks = list(list_chunks(tracks_info,chunk_size))
	# return artists_info_lists
	url_chunks = []
	key_chunks = []
	for each in tracks_info_chunks:
		(a,b) = zip(*each)
		url_chunks.append(a)
		key_chunks.append(b)
	print len(url_chunks)

	num_cores = multiprocessing.cpu_count() #- 1
	# print num_cores
	
	outputDict = {}
	results = Parallel(n_jobs= (num_cores ) )(delayed(async_get)(urls , idx) for idx,urls in enumerate(url_chunks))



	jumbled_url_chunks = []
	response_chunks = []

	for each in results:
		jumbled_url_chunks.append(each[1])
		response_chunks.append(each[0])

	# total_video_content_details = []
	errs = []

	for idx,jumbled_urls in enumerate(jumbled_url_chunks):
		for idx2, q_url in enumerate(jumbled_urls):
			playlist_details = []

			try:
				loc = url_chunks[idx].index(q_url)
				movie_name = key_chunks[idx][loc]

				resp = response_chunks[idx][idx2].json()
				
				# print resp['items'][0]['id']

				for itm in resp['items']:
					# print itm
					# print type(itm)
					# total_video_content_details.append(itm['contentDetails'])
					playlist_details.append({
						'id': itm['id']['playlistId'],
						'title':itm['snippet']['title'],
						'description':itm['snippet']['description']
						})
						# for id_info in itm:
							# print id_info
							# id_info = json.loads(id_info)
							# print type(id_info)
					
					# if len(video_ids) == 0:
						# print movie_name
						# print track_index
						# print movie_song_names[movie_name]['track_list'][track_index]['track_name']
						# print movie_song_names[movie_name]['track_list'][track_index]['search_type_'+ str(search_type)]['url']
						# print q_url
						# errs.append(response_chunks[idx][idx2])
			except:
				pass

			movie_song_names[movie_name]['search_type_'+ str(search_type)]['playlist_details'] = playlist_details

	out_file = in_file.replace('.json','_playlist_details.json')
	out_file_pointer = open(out_file,'wb')
	json.dump(movie_song_names,out_file_pointer)
	return 'done'
	# return movie_song_names

	# for artists_chunk in list_chunks(artist_info,chunk_size):
	# 	url_chunk,id_chunk = zip(*artists_chunk)

	# 	responses, jumbled_urls = async_get(url_chunk)		

	# 	ordered_responses = []
	# 	for idx,url in enumerate(url_chunk):
	# 		loc = jumbled_urls.index(url)
	# 		ordered_responses.append(responses[loc])

# def get_videos_duration(in_file = 'consolidated_song_names_with_videoIds.json', chunk_size = 800, search_type = 1 ):
# 	(url_list,key_list,movie_song_names) = url_constructor(in_file,1)	





