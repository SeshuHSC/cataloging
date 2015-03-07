import os,sys
import time
# import url_parser
import urllib2
import string
import json
from simple_requests import Requests
import csv
from joblib import Parallel, delayed
import multiprocessing
from multiprocessing import Process, Manager
import urllib
import dateutil.parser
import os.path

jumbled_urls_dict = {}
requests = Requests()

def get_full_song_path(movie_url, song_url, out_dir = '/home/varun01124/code/bollywoodSongsGather/songs'):
	
	movie_name = movie_url.split('/')[-1][:-5]
	movie_path = os.path.join(out_dir,movie_name)
	song_name = song_url.split('/')[-1]
	final_path = os.path.join(movie_path,song_name)
	return movie_path, final_path

def list_chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def write_mp3(mp3, fullpath=None):
        """
        Writes the downloaded file and renames the title.

        """
        print 'saving ' + fullpath
        with open(fullpath, 'w') as output:
            # while True:
                # buf = mp3.read(65536)  # Fixed the Buffer size
            buf = mp3.iter_content(65536)
            # if not buf:
            #     break
            for chunk in buf:
            	output.write(chunk)

def check_if_downloaded(url, out_dir):

	movie_url, song_num = jumbled_urls_dict[url]['details']
	movie_path, final_path = get_full_song_path(movie_url, url, out_dir)
	return os.path.isfile(final_path), final_path

def store_response_chunk_disk(responses, jumbled_urls, out_dir):

	for idx, url in enumerate(jumbled_urls):
		movie_url, song_num = jumbled_urls_dict[url]['details']
		movie_path, final_path = get_full_song_path(movie_url, url, out_dir)
		if not os.path.exists(movie_path):
			os.makedirs(movie_path)
		# try:
		write_mp3(responses[idx],final_path)
		# global jumbled_urls_dict
		# jumbled_urls_dict[url]['downloaded'] = True
		# global jumbled_urls_dict
		# jumbled_urls_dict[url]['final_path'] = final_path
		# except:
		# 	jumbled_urls_dict[url]['final_path'] = final_path
		# 	jumbled_urls_dict[url]['downloaded'] = False

def async_get(url_list, idx, out_dir):
	""" Make async get requests to a list of urls and returns the responses and the urls( which might not be in the original order as the input).
	"""
	responses = []
	jumbled_urls = []
	print len(url_list)
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

	store_response_chunk_disk(responses, jumbled_urls, out_dir)

	# return responses, jumbled_urls
	return jumbled_urls



def url_constructor(in_file = 'consolidated_song_names.json', search_type = 1, download_details = 'url_location_map.json', out_dir = '/asdasd'):
	""" Construct search urls for tracks. 
		search_type = 1 : Search by track_name
	"""
	input_pointer = open(in_file,'rb')
	movie_song_names = json.load(input_pointer, encoding = 'ISO-8859-1')

	# try:
	# 	earlier_download_details = json.load(open(download_details), encoding = 'ISO-8859-1')
	# except:
	# 	earlier_download_details = {}

	url_list = []
	# artist_ids = []
	key_list = [] #store reverse search info
	
	# if search_type == 1:
	# jumbled_urls_dict = {}
	for idx,key in enumerate(movie_song_names.keys()):
		# print key
		for idx2,track in enumerate(movie_song_names[key]):
			# url = youtubeGetChannelIdUrl
			# print rw[1].decode('utf-8')
			# print rw[1]
			# print track['track_name']
			
			# track_index_list.append()
			if track != [] and track[1] != '':
				# downloaded = False
				movie_path, final_path = get_full_song_path(key, track[1], out_dir)
				# if earlier_download_details.has_key(track[1]):
				# 	downloaded = earlier_download_details[track[1]]['downloaded']
				# if not downloaded:
				if final_path[-3:] in ['wav','mp3'] and not (os.path.isfile(final_path)):
					jumbled_urls_dict[track[1]] = {}
					jumbled_urls_dict[track[1]]['details'] = (key,idx2)
					url_list.append(track[1])
					key_list.append((key,idx2))


				# artist_ids.append(rw[0])

	return (url_list, key_list, movie_song_names)

def download_songs(in_file = 'songsPk_resolved.json',out_dir = '/home/varun01124/code/bollywoodSongsGather/songs', chunk_size = 20, search_type = 1 ):
	""" Search youtube for artist names and select channel with most subscribers only if it has more than min_subscribers 
	"""
	(url_list,key_list,movie_song_names) = url_constructor(in_file,1,'url_location_map.json', out_dir)

	# print url_list
	tracks_info = zip(url_list,key_list)

	tracks_info_chunks = list(list_chunks(tracks_info,chunk_size))
	# return artists_info_lists
	# print tracks_info_chunks
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
	results = Parallel(n_jobs= (num_cores ) )(delayed(async_get)(urls , idx, out_dir) for idx,urls in enumerate(url_chunks))

	# json.dump(jumbled_urls_dict, open('url_location_map.json','wb'))

	jumbled_url_chunks = []
	# response_chunks = []

	for each in results:
		jumbled_url_chunks.append(each[1])

	song_location_details = json.load(open('url_location_map.json'), encoding = 'ISO-8859-1')
	for chnk in jumbled_url_chunks:
		for url in chnk:
			downloaded, final_path = check_if_downloaded(url, out_dir)
			song_location_details[url] = {}
			song_location_details[url]['details'] = jumbled_urls_dict[url]['details']
			song_location_details[url]['downloaded'] = downloaded
			song_location_details[url]['location'] = final_path

	json.dump(song_location_details, open('url_location_map.json','wb'))
			
	# 	response_chunks.append(each[0])

	# total_video_ids = []
	# errs = []
	# for idx,jumbled_urls in enumerate(jumbled_url_chunks):
	# 	for idx2, q_url in enumerate(jumbled_urls):
	# 		video_ids = []
	# 		video_infos = []

	# 		try:
	# 			loc = url_chunks[idx].index(q_url)
	# 			(movie_name,track_index) = key_chunks[idx][loc]

	# 			resp = response_chunks[idx][idx2].json()
				
	# 			# print resp['items'][0]['id']

	# 			for itm in resp['items']:
	# 				# print itm
	# 				# print type(itm)
	# 				# total_video_ids.append(itm['id']['videoId'])
	# 				video_ids.append(itm['id']['videoId'])
	# 				video_infos.append(itm['snippet'])
	# 				# for id_info in itm:
	# 					# print id_info
	# 					# id_info = json.loads(id_info)
	# 					# print type(id_info)
				
	# 			# if len(video_ids) == 0:
	# 				# print movie_name
	# 				# print track_index
	# 				# print movie_song_names[movie_name]['track_list'][track_index]['track_name']
	# 				# print movie_song_names[movie_name]['track_list'][track_index]['search_type_'+ str(search_type)]['url']
	# 				# print q_url
	# 				# errs.append(response_chunks[idx][idx2])
	# 		except:
	# 			pass

	# 		movie_song_names[movie_name]['track_list'][track_index]['search_type_'+ str(search_type)]['video_ids'] = video_ids
	# 		movie_song_names[movie_name]['track_list'][track_index]['search_type_'+ str(search_type)]['video_infos'] = video_infos

	# out_file = in_file.replace('.json','_with_videoIds.json')
	# out_file_pointer = open(out_file,'wb')
	# json.dump(movie_song_names,out_file_pointer)
	return 'done'


def bulk_resolver():
    k = json.load(open('SongsPKData.json'))
    movs = {}
    for idx,movi in enumerate(k):
	    nam = movi.keys()[0]
	    movs[nam] = []
	    for ech in movi[nam]:
	        try:
	            movs[nam].append((ech,url_resolver(str(ech))))
	        except:
	            movs[nam].append((ech,''))
	    print idx
	    # print movs[nam]
    # json.dump(movs,open('songs_pk_resolved.json','wb'))
    return movs


def url_resolver(url):
    opener = urllib2.build_opener()
    opener.addheaders = [('User-agent', 'Mozilla/5.0')]
    res = opener.open(urllib2.Request(url))  # Resolve the redirects and gets the song Object
    return res.geturl()

# def download_all_songs(input_file = '', chunk_size = '20'):
if __name__ == "__main__":
	download_songs('songsPk_resolved.json', '/mnt/bollywoodCollection', 20)	