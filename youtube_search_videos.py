from simple_requests import Requests
import csv
from joblib import Parallel, delayed
import multiprocessing
from multiprocessing import Process, Manager
import urllib
import json

requests = Requests()


youtubeKey = 'AIzaSyCleMRxeVkRDzBHDGWvUk-gBu7qPLrMCj8'
youtubeGetChannelIdUrl = "https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&maxResults=2&key=" + youtubeKey + "&q=" #"https://www.googleapis.com/youtube/v3/search?part=snippet&type=channel&maxResults=2&key=AIzaSyCg0kT3wqdZfjHz1a1vJzZvdpL28rMiRQA&q="
youtube_get_video = "https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&maxResults=5&key=" + youtubeKey + "&q="
youtubeGetActivityUrl = "https://www.googleapis.com/youtube/v3/activities?part=snippet%2CcontentDetails&maxResults=5&key=" + youtubeKey + "&channelId=" #AIzaSyBpMTMC61LBMinww2etKXw2CrqwtbWjemI"
youtubeGetVideoStats = "https://www.googleapis.com/youtube/v3/videos?part=statistics" + "&key=" + youtubeKey + "&id="
youtubeGetChannelStats = "https://www.googleapis.com/youtube/v3/channels?part=statistics" + "&key=" + youtubeKey + "&id="

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
				key_list.append((key,idx2))
				# track_index_list.append()
				if track['track_name'] != None:
					url = youtube_get_video + urllib.quote_plus(track['track_name'].encode('utf8'))
					movie_song_names[key]['track_list'][idx2]['search_type_'+ str(search_type)] = {}
					movie_song_names[key]['track_list'][idx2]['search_type_'+ str(search_type)]['url'] = [url]
					url_list.append(url)

				else:
					movie_song_names[key]['track_list'][idx2]['search_type_'+ str(search_type)] = {}
					movie_song_names[key]['track_list'][idx2]['search_type_'+ str(search_type)]['url'] = ['']
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
	# print idx*len(url_list)
	print idx
	# print len()

	for response in requests.swarm(url_list, maintainOrder = False):
		responses.append(response)
		jumbled_urls.append(response.url)
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
	results = Parallel(n_jobs= (num_cores - 1) )(delayed(async_get)(urls , idx) for idx,urls in enumerate(url_chunks))



	jumbled_url_chunks = []
	response_chunks = []

	for each in results:
		jumbled_url_chunks.append(each[1])
		response_chunks.append(each[0])

	total_video_ids = []
	errs = []
	for idx,jumbled_urls in enumerate(jumbled_url_chunks):
		for idx2, q_url in enumerate(jumbled_urls):
			loc = url_chunks[idx].index(q_url)
			(movie_name,track_index) = key_chunks[idx][loc]

			resp = response_chunks[idx][idx2].json()
			video_ids = []
			# print resp['items'][0]['id']

			for itm in resp['items']:
				# print itm
				# print type(itm)
				total_video_ids.append(itm['id']['videoId'])
				video_ids.append(itm['id']['videoId'])
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

			movie_song_names[movie_name]['track_list'][track_index]['search_type_'+ str(search_type)]['video_ids'] = video_ids

	out_file = in_file.replace('.json','_with_videoIds.json')
	out_file_pointer = open(out_file,'wb')
	json.dump(movie_song_names,out_file_pointer)	
	# ordered_response_chunks = []
	# for idx,each in enumerate(url_chunks):
	# 	ordered_response = []
	# 	for each_url in each:
	# 		loc = jumbled_url_chunks[idx].index(each_url)
	# 		ordered_response.append(response_chunks[idx][loc])
	# 	ordered_response_chunks.append(ordered_response)

	# response_json_chunks = []

	# for idx,each in enumerate(ordered_response_chunks):
	# 	response_lists = []
	# 	for idx2, resp in enumerate(each):
	# 		res = resp.json()
	# 		res['artist_id'] = id_chunks[idx][idx2]
	# 		response_lists.append(res)
	# 	response_json_chunks.append(response_lists)


	return 'done'
	# return movie_song_names

	# for artists_chunk in list_chunks(artist_info,chunk_size):
	# 	url_chunk,id_chunk = zip(*artists_chunk)

	# 	responses, jumbled_urls = async_get(url_chunk)		

	# 	ordered_responses = []
	# 	for idx,url in enumerate(url_chunk):
	# 		loc = jumbled_urls.index(url)
	# 		ordered_responses.append(responses[loc])





