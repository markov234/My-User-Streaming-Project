
# fix issue with kafka-python library that hasn't been updated to support Python 3.12 (since about November 2023)
import sys
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaProducer
import json
import requests
import time

def get_random_user():
    response = requests.get('https://randomuser.me/api/')
    results = response.json()['results'][0]     # get data as dictionary

    return results

def format_data(results):
    data = {}

    data['id'] = results['login']['uuid']
    data['first_name'] = results['name']['first']
    data['last_name'] = results['name']['last']
    data['gender'] = results['gender']
    data['address'] = f"{results['location']['street']['number']} {results['location']['street']['name']}" 
    data['city'] = results['location']['city'] 
    data['state'] = results['location']['state'] 
    data['country'] = results['location']['country'] 
    data['postcode'] = results['location']['postcode']
    data['email'] = results['email']
    # data['dob'] = results['dob']['date']
    data['age'] = results['dob']['age']
    # data['registered_date'] = results['registered']['date']
    data['phone'] = results['phone']
    data['picture'] = results['picture']['medium']

    return data

def run_kafka_producer():
    producer = KafkaProducer(bootstrap_servers='localhost:9092', 
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    topic = 'randomusers'
    # Read somewhere that underscores in topic name might cause problems, so try to avoid underscores.
    start_time = time.time()
    produce_time = 2    # seconds. From my estimate, about 5-10 messages produced from API per second.
    print(f"Producing messages to topic {topic} for {produce_time} seconds")

    while time.time() - start_time < produce_time:
        results = get_random_user()
        data = format_data(results)

        producer.send(topic, value=data)
    
    producer.flush()
    producer.close()
    print("Producer closed")

if __name__ == '__main__':

    run_kafka_producer()
