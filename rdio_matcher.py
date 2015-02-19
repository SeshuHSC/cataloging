#!/usr/bin/env python
import oauth2 as oauth
import urllib, cgi
from joblib import Parallel, delayed
import multiprocessing
import pickle
import json

CONSUMER_KEY = 'we9vdxdc3btz5fxr5wemtnzs' #'72k6684k6r2tuwd5vujusa4z'	#
CONSUMER_SECRET = 'HgcNuMdR5Q' #'3QBqTyuGjt'	#

source_info =  {
    "album_confidence": -1,
    "song_confidence": -1,
    "is_selected": False,
    "source_name": "",
    "album_id": ""
    }

artist_info =  {
    "artist_id": "",
    "artist_name": ""
    }

album_info = {
    "source_info": [],
    "thumbnail_info": [],
    "artist": [],
    "track_list": [],
    "year": -1,
    "album_name": ""
    }
track_info =  {
    "is_explicit": False,
    "track_name": "",
    "track_id": "",
    "length": -1,
    "local_path": "",
    "url_audio": ""
    }
thumbnail_info = {
    "url": "",
    "size": ""
    }

def initial_authorisation():
	# create the OAuth consumer credentials
	'''To be used for initial authorisation if the current access_token stored in accs_token.p has expired or doesn't work '''
	consumer = oauth.Consumer(CONSUMER_KEY, CONSUMER_SECRET)

	# make the initial request for the request token
	client = oauth.Client(consumer)
	response, content = client.request('http://api.rdio.com/oauth/request_token', 'POST', urllib.urlencode({'oauth_callback':'oob'}))
	parsed_content = dict(cgi.parse_qsl(content))
	request_token = oauth.Token(parsed_content['oauth_token'], parsed_content['oauth_token_secret'])

	# ask the user to authorize this application
	print 'Authorize this application at: %s?oauth_token=%s' % (parsed_content['login_url'], parsed_content['oauth_token'])
	oauth_verifier = raw_input('Enter the PIN / OAuth verifier: ').strip()
	# associate the verifier with the request token
	request_token.set_verifier(oauth_verifier)

	# upgrade the request token to an access token
	client = oauth.Client(consumer, request_token)
	response, content = client.request('http://api.rdio.com/oauth/access_token', 'POST')
	parsed_content = dict(cgi.parse_qsl(content))
	access_token = oauth.Token(parsed_content['oauth_token'], parsed_content['oauth_token_secret'])

	# make an authenticated API call
	client = oauth.Client(consumer, access_token)
	response = client.request('http://api.rdio.com/1/', 'POST', urllib.urlencode({'method': 'currentUser'}))
	
	pickle.dump(client,open('client.p','wb'))
	pickle.dump(access_token,open('accs_token.p','wb'))
	# print response[1]
	# return (access_token,client)
	return 'access_token and client pickled'


def get_tracks(movie_name = 'lagaan', movie_yr = '2001'):
	consumer = oauth.Consumer(CONSUMER_KEY, CONSUMER_SECRET)
	access_token = pickle.load(open('accs_token.p','rb'))
	client = pickle.load(open('client.p','rb'))

	# print movie_name.encode('utf-8') +' '+ str(movie_yr)
	response = client.request('http://api.rdio.com/1/', 'POST', urllib.urlencode({'method': 'search','query':movie_name.encode('utf-8') ,'types':'Album'}))
	# return response[1]
	# print movie_name
	# # print response
	# album_info = {
	# 			'album_name': '',
	# 			'thumbnail':'',
	# 			'release_date':'',
	# 			'artist':'',
	# 			'tracks': []
	# 			}

	# trck = { "is_explicit": false,"track_name": "","track_id": "","length": -1,"local_path": "","url_audio": ""}

	album_info = {
	    "source_info": { "album_confidence": -1,"song_confidence": -1,"is_selected": False,"source_name": "rdio","album_id": ""},
	    "thumbnail_info": [{"url": "", "size": ""}],
	    "artist": [{"artist_id": "","artist_name": "", "artist_info":{"role":''}}],
	    "track_list": [],
	    "release_date": -1,
	    "album_name": ""
		}

	# track_info =  {
	#     "is_explicit": False,
	#     "track_name": "",
	#     "track_id": "",
	#     "length": -1,
	#     "local_path": "",
	#     "url_audio": ""
	#     }	
	# print 'asdadas'
	album_found = 0
	tracks_found = 0

	try:
		search_response = json.loads(response[1])


		# print search_response['result']
		if search_response['result']['number_results'] != 0:
			# print search_response['result']
			# print search_response['result']['results'][0]
			track_ids = search_response['result']['results'][0]['trackKeys']

			album_info["thumbnail_info"][0]["url"] =  search_response['result']['results'][0]['icon']
			album_info["artist"][0]["artist_name"] =  search_response['result']['results'][0]['artist']
			album_info["artist"][0]["artist_id"] =  search_response['result']['results'][0]['artistKey']
			album_info["album_name"] =  search_response['result']['results'][0]['name']
			album_info["release_date"] =  search_response['result']['results'][0]['releaseDate']
			album_info["source_info"]["album_id"] =  search_response['result']['results'][0]['key']


			search_string = ''			

			for tr in track_ids:
				search_string += tr +','

			# print search_string
			response = client.request('http://api.rdio.com/1/', 'POST', urllib.urlencode({'method': 'get','keys':search_string}))
			track_info = json.loads(response[1])
			# print track_info
			if track_info['status']== 'ok':
				for track_id in track_ids:
					# print 'asd'
					# print track_info['result'][track_id]
					album_info['track_list'].append({'track_name':track_info['result'][track_id]['name'],'is_explicit':track_info['result'][track_id]['isExplicit'], 'track_id': track_id, 'length': track_info['result'][track_id]['duration'], 'artist': [{"artist_id": track_info['result'][track_id]['artistKey'],"artist_name": track_info['result'][track_id]['artist'], "artist_info":{"role":''} }]})
					# print response[1]
					# track_info = json.loads(response[0])
					# album_info['tracks'].append(track_info)
				album_found = 1
				tracks_found = 1
				print 'tracks found'
	
	except :
		pass

	return (album_info, album_found,tracks_found)

def query_movie_list(in_file = 'bollywoodMovies.json',out_file = 'movie_song_info_rdio.json'):

	movie_file = json.load(open(in_file,'rb'))

	movies = []

	for yr in movie_file.keys():
		# if int(yr) >= 2001 and int(yr) <= 2014:
		print yr
		# print movie_file[yr]
		for movie_name in movie_file[yr].keys():
			movies.append([movie_name,yr])

	# return movies


	all_movie_info = {}
	# print movies[0:20]
	album_total_found = 0
	tracks_total_found = 0

	for idx,movie in enumerate(movies):
		print idx
		print movie
		# print 'asdasd'
		(info,album_found,tracks_found) = get_tracks(movie[0], movie[1])
		album_total_found += album_found
		tracks_total_found += tracks_found
		all_movie_info[movie[0]] = info

	print 'albums and tracks found'
	print album_total_found
	print ''

	print 'album found but tracks not found'
	print tracks_total_found
	print ''

	print 'total input movies'
	print len(movies)
	json.dump(all_movie_info,open(out_file,'wb'), indent=4)

	 # m['result']['results'][0]['trackKeys']