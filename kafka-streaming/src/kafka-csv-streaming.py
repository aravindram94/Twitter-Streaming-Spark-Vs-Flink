import json
from kafka import KafkaProducer, SimpleClient
import csv

if __name__ == '__main__':

    client = SimpleClient("localhost:9092")
    producer = KafkaProducer()

    with open('tweets.csv') as csvDataFile:
        csvReader = csv.reader(csvDataFile)
        for row in csvReader:
            data = {};
            data['id'] = row[0]
            data['text'] = row[1]
            data['user_id'] = row[2]
            data['timestamp_ms'] = row[3]
            data['place_full_name'] = row[4]
            data = json.dumps(data)
            print(data)
            producer.send('tweets', data.encode('utf-8'))

