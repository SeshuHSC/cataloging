#!/usr/bin/env python
import oauth2 as oauth
import urllib, cgi
from joblib import Parallel, delayed
import multiprocessing
import pickle
import json
import urllib3

base_url = 'http://api.gaana.com/index.php?'

gaana_album_base = 'http://s.staging.api.gaana.com/index.php?type=album&subtype=all_albums&language=hindi&limit='
# http://api.gaana.com/index.php?type=album&subtype=all_albums&limit=8401,8500&language=hindi
gaana_track_base = 'http://s.staging.api.gaana.com/index.php?type=song&subtype=song_detail&track_id='
# http://api.gaana.com/index.php?type=song&subtype=song_detail&track_id=3328214

gaana_search_base = 'http://s.staging.api.gaana.com/index.php?type=search&subtype=search_album&key='
#http://s.staging.api.gaana.com/index.php?type=search&subtype=search_album&key=dil+de+chuke+sanam


def get_tracks_info(tracks):
	tracks = tracks.replace(' ','')
	url = gaana_track_base + tracks
	
	# print url
	http = urllib3.PoolManager()
	r = http.request('GET',url)
	response = json.loads(r.data)
	all_tracks_info = []
	# print response

	try:
		for track_info in response['tracks']:
			info = 	{
					'track_name':track_info['track_title'],
					'is_explicit':'unknown', 
					'track_id': track_info['track_id'], 
					'length': track_info['duration'], 
					'artist': [] 
					} 

			for artist in track_info['artist']:
				info['artist'].append({"artist_id": artist['artist_id'],"artist_name": artist['name'], "artist_info":{"role":''} })
			all_tracks_info.append(info)

	except:
		pass

	return all_tracks_info


def get_album_list(limits = [-1,8487],  out_file = 'all_hindi_albums_gaana.json'):

	'''Iterates over all the albums and all the tracks available from the all albums endpoint of gaana'''

	iters = range(limits[0],limits[1],100)
	all_info = []
	k = 0
	for idx,val in enumerate(iters):
		if idx == len(iters) -1:
			break

		url = gaana_album_base + str(val+1) + ',' + str(iters[idx+1]) 
		# url += uid
		# print url

		#response = json.load( urllib2.urlopen( url ) )
		#print type(urllib2.urlopen( url ))
		#print "\n***Processing " + track_file + "\n"
		http = urllib3.PoolManager()
		r = http.request('GET',url)
		#print type(r)
		response = json.loads(r.data)
		#print response
		#response = json.load( urllib2.urlopen( url ) )
		#print response
		# channelId = None


		# try:

		for album in response['album']:
			# print album
			k += 1
			# print k
			album_info = {
			    "source_info": { "album_confidence": -1,"song_confidence": -1,"is_selected": False,"source_name": "gaana","album_id": ""},
			    "thumbnail_info": [{"url": "", "size": ""}],
			    "artist": [], #{"artist_id": "","artist_name": "", "artist_info":{"role":''}}
			    "track_list": [],
			    "release_date": -1,
			    "album_name": ""
				}

			track_ids = album['trackids'].split(',')
			# print track_ids
			album_info["thumbnail_info"][0]["url"] =  album["custom_artworks"]["480x480"]
			album_info["thumbnail_info"][0]["size"] =  "480x480"
			
			for artist in album["artist"]:
				album_info['artist'].append({"artist_id": artist['artist_id'],"artist_name": artist['name'], "artist_info":{"role":''}})

			for artist in album["primaryartist"]:
				album_info['artist'].append({"artist_id": artist['artist_id'],"artist_name": artist['name'], "artist_info":{"role":'primary_artist_gaana'}})

			album_info["album_name"] =  album['title']
			album_info["release_date"] =  album['release_date']
			album_info["source_info"]["album_id"] =  album['album_id']

			tr = ''
			for track in track_ids:
				tr += str(track) + ','

			# print tr	
			album_info["track_list"] = get_tracks_info(tr)
			# all_info[album_info["album_name"]] = album_info
			all_info.append(album_info)
		# except:
		# 	pass

	json.dump(all_info,open(out_file,'wb'), indent=4)


def search_album_info(movie_name = 'lagaan', movie_yr = '2001'):
	'''Search for the query in the gaana db and get tracks for first returned album.'''

	query = movie_name.encode('utf-8').replace(' ','+') + '+' + str(movie_yr)

	url = gaana_search_base + query
	
	# print url
	
	# all_tracks_info = []
	# print response

	album_info = {
	    "source_info": { "album_confidence": -1,"song_confidence": -1,"is_selected": False,"source_name": "gaana","album_id": ""},
	    "thumbnail_info": [{"url": "", "size": ""}],
	    "artist": [], #{"artist_id": "","artist_name": "", "artist_info":{"role":''}}
	    "track_list": [],
	    "release_date": -1,
	    "album_name": ""
		}

	album_found = 0
	tracks_found = 0
	
	try:
		http = urllib3.PoolManager()
		r = http.request('GET',url)
		response = json.loads(r.data)
		if int(response['count']) > 0 :
			
			album = response['album'][0]
			track_ids = album['trackids'].split(',')
			# print track_ids
			album_info["thumbnail_info"][0]["url"] =  album["custom_artworks"]["480x480"]
			album_info["thumbnail_info"][0]["size"] =  "480x480"
			
			
			for artist in album["artist"]:
				album_info['artist'].append({"artist_id": artist['artist_id'],"artist_name": artist['name'], "artist_info":{"role":''}})

			for artist in album["primaryartist"]:
				album_info['artist'].append({"artist_id": artist['artist_id'],"artist_name": artist['name'], "artist_info":{"role":'primary_artist_gaana'}})

			# except:
			# 	pass

			album_info["album_name"] =  album['title']
			album_info["release_date"] =  album['release_date']
			album_info["source_info"]["album_id"] =  album['album_id']

			tr = ''
			for track in track_ids:
				tr += str(track) + ','

			# print tr	
			album_info["track_list"] = get_tracks_info(tr)

			album_found = 1
			tracks_found = len(album_info["track_list"])
		# all_info[album_info["album_name"]] = album_info

	except:
		pass

	return (album_info, album_found, tracks_found)


def query_movie_list(in_file = 'bollywoodMovies.json',out_file = 'movie_song_info_gaana.json'):

	movie_file = json.load(open(in_file,'rb'))

	movies = []

	for yr in movie_file.keys():
		# if int(yr) >= 2001 and int(yr) <= 2014:
		print yr
		# print movie_file[yr]
		for movie_name in movie_file[yr].keys():
			movies.append([movie_name,yr])

	# return movies


	# all_movie_info = {}
	all_movie_info = []
	# print movies[0:20]
	album_total_found = 0
	tracks_total_found = 0

	for idx,movie in enumerate(movies):
		print idx
		print movie
		# print 'asdasd'
		(info,album_found,tracks_found) = search_album_info(movie[0], movie[1])
		print album_found
		print tracks_found
		album_total_found += album_found
		tracks_total_found += tracks_found
		all_movie_info.append(info)

	print 'albums and tracks found'
	print album_total_found
	print ''

	print 'album found but tracks not found'
	print tracks_total_found
	print ''

	print 'total input movies'
	print len(movies)
	json.dump(all_movie_info,open(out_file,'wb'), indent=4)