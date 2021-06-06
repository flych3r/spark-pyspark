import os
import sys
import socket
import json

import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener


CONSUMER_KEY = os.getenv('CONSUMER_KEY')
CONSUMER_SECRET = os.getenv('CONSUMER_SECRET')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.getenv('ACCESS_TOKEN_SECRET')


class TweetsListener(StreamListener):

    def __init__(self, client_socket):
        self.client_socket = client_socket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print('Error on_data: %s' % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def send_data(c_socket, topic):
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=[topic])


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(
            "Usage: tweet_stream.py <port> <topic>",
            file=sys.stderr
        )
        sys.exit(-1)

    s = socket.socket()         # Create a socket object
    host = '127.0.0.1'          # Get local machine name
    port = int(sys.argv[1])     # Reserve a port for your service.
    s.bind((host, port))        # Bind to the port
    topic = sys.argv[2]         # Set the topic to stream

    print(f'Listening on port: {port}')
    print(f'Ready to stream topic {topic}')

    s.listen(5)                 # Now wait for client connection.
    c, addr = s.accept()        # Establish connection with client.

    print(f'Received request from: {addr}')

    send_data(c, topic)
