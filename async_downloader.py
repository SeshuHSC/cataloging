import os,sys
import time
# import url_parser
import urllib2
import string
import json
from simple_requests import Requests
import csv


def list_chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def async_get(url_list):
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

	return responses, jumbled_urls

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
	