import os,sys
import time
# import url_parser
import urllib2
import string
import json
import requests
# from utils import url_resolver
import multiprocessing as mp

def getAllMovieLinks():
	alphabets = list(string.ascii_lowercase)
	obj = url_parser.URLParserHref()
	movies = []
	songs = []
	for a in alphabets:
		html = urllib2.urlopen('http://www.songspk.name/%s_list.html'%a)
		try:
			movies_local = obj.get_movie_names(html)
		except:
			movies_local =[]
			pass
		movies.extend(movies_local)
		for m in movies_local:

			try:
				html = urllib2.urlopen('http://www.songspk.name/'+m)
				songs_local = obj.get_songs_url(html)
			except:
				songs_local = []
				pass
			songs.append({m:songs_local})


	json.dump(songs, open('data.json','w'))

def write_mp3(mp3, fullpath=None):
        """
        Writes the downloaded file and renames the title.

        """
        with open(fullpath, 'w') as output:
            # while True:
                # buf = mp3.read(65536)  # Fixed the Buffer size
            buf = mp3.iter_content(65536)
            # if not buf:
            #     break
            for chunk in buf:
            	output.write(chunk)

def downloadASong(moviePath, sURL):
	res, finalurl = url_resolver(sURL)
	songName = finalurl.split('/')[-1]
	finalPath = os.path.join(moviePath,songName)
	fid = open('downloadLogs.txt','a')
	try:
		if finalurl.endswith('.mp3') and not finalurl.startswith('..'):
			write_mp3(res, finalPath)
			fid.write("%s\t%s\t%d\n"%(moviePath, sURL,1))
		else:
			fid.write("%s\t%s\t%d\n"%(moviePath, sURL,0))
	except:
		print finalurl
		fid.write("%s\t%s\t%d\n"%(moviePath, sURL,0))
	fid.close()
	#fid = open(finalPath,'w')
	#mp3Data = requests.get(sURL)
	#fid.write(mp3Data.content)

def downloadAllSongsPK(dataFile, outDir):
	fid = open('downloadLogs.txt','w')
	fid.close()
	data = json.load(open(dataFile,'r'))
	for d in data[:10]:
		print d
		key = d.keys()[0]
		movieName = key.split('/')[-1][:-5]
		moviePath = os.path.join(outDir,movieName)
		if not os.path.exists(moviePath):
			os.makedirs(moviePath)
		try:
			pArray = []
			for sURL in d[key]:
				print sURL
				pArray.append(mp.Process(target = downloadASong, args = (moviePath, sURL)))
			for p in pArray:
				p.start()
			for p in pArray:
				p.join()
		except:
			pass





