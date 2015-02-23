from pprint import pformat

from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol
from twisted.web.client import Agent
from twisted.web.http_headers import Headers
import json

class BeginningPrinter(Protocol):
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

agent = Agent(reactor)
d = agent.request(
    'GET',
    'http://api.musixmatch.com/ws/1.1/track.get?apikey=b463ed1270b71853d56be5bd776a9b4a&track_id=30447195',
    Headers({'User-Agent': ['Twisted Web Client Example']}),
    None)

def cbRequest(response):
    # print 'Response version:', response.version
    # print 'Response code:', response.code
    # print 'Response phrase:', response.phrase
    # print 'Response headers:'
    # print pformat(list(response.headers.getAllRawHeaders()))
    # return response.deliverBody()
    finished = Deferred()
    response.deliverBody(BeginningPrinter(finished))
    return finished
d.addCallback(cbRequest)

def cbShutdown(resp):
	# print 
	k = json.loads(resp)
	m = k['message']['body']['track']['track_edit_url']
	f = Deferred()
	f.callback(m)
	return f
def shut(resp):
	# print 'asd'
	print resp
	reactor.stop()
d.addCallback(cbShutdown)
d.addCallback(shut)

reactor.run()