import json
import csv
from kafka import KafkaProducer, SimpleClient
from tweepy import OAuthHandler, Stream, API
from tweepy.streaming import StreamListener
from configparser import ConfigParser
from pymongo import MongoClient

csvFile = open('tweets.csv', 'a')
csvWriter = csv.writer(csvFile)
# csvWriter.writerow(["id", "text", "user_id", "timestamp_ms", "place_full_name"])

class TstreamListener(StreamListener):


    def twitter_data_parser(self, data):
        selected_fields = {}

        selected_fields['id'] = data['id_str']

        selected_fields['text'] = data['text']

        selected_fields['user_id'] = data['user']['id']

        selected_fields['timestamp_ms'] = data['timestamp_ms']

        if 'place' in data:
            place_data = data['place']
            if 'full_name' in place_data:
                selected_fields['place_full_name'] = data['place']['full_name']

        return selected_fields

    def on_data(self, data):
        """
        Called whenever new data arrives as live stream
        """
        print(data)
        data = json.loads(data)
        if data['lang'] == 'en' and 'text' in data:
            data = self.twitter_data_parser(data)
            try:
                csvWriter.writerow(data.values())
            except Exception as e:
                print(e)
                return False
        return True

    def on_error(self, status_code):
        print("Error received in kafka producer")
        return True  # don't kill the stream

    def on_timeout(self):
        return True


if __name__ == '__main__':
    # authenticate
    config = ConfigParser()
    config.read('../config/credentials.ini')
    consumer_key = config.get('authentication', 'consumer_key')
    consumer_secret = config.get('authentication', 'consumer_secret_key')
    access_token = config.get('authentication', 'access_token')
    access_token_secret = config.get('authentication', 'access_token_secret')

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = API(auth)

    stream = Stream(auth, listener=TstreamListener())

    # lat and long of USA boundaries
    # 49.3457868  # north lat
    # -124.7844079  # west long
    # -66.9513812  # east long
    # 24.7433195  # south lat
    i = 0
    while i < 10:
        i += 1
        try:
            stream.filter(locations=[-124.7844079, 24.7433195, -66.9513812, 49.3457868])
        except Exception as e:
            print(e)

