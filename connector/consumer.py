# implements Kafka topic consumer functionality

from cgitb import reset
from datetime import datetime
import math
import multiprocessing
from random import randrange
import threading
import time
from confluent_kafka import Consumer, OFFSET_BEGINNING
from flask import request
import json
from producer import proceed_to_deliver


_requests_queue: multiprocessing.Queue = None
            

def handle_event(id, details_str):
    details = json.loads(details_str)
    print(f"[info] handling event {id}, {details['source']}->{details['deliver_to']}: {details['operation']}")
    try:
        delivery_required = False
        if details['operation'] == 'acts':
            #response = request.post('https://localhost:6004/confirmation', json={'status':'successfull'})
            print(f"[acts] event {id}, ICS returned acts to coordinator")
            print(f"{details}]")
            print(f"Recipe - to mix: ")
            for x in details['mix']:
                ind = details['mix'].index(x)
                print(str(ind+1) + '. ' + x + " from " + details['from'][ind] + " in quantity " + str(details['amount'][ind]))
            print(f"Using: ")
            for x in details['using']:
                ind = details['using'].index(x)
                print(str(ind+1) + '. ' + x)
            for x in details['rules']:
                ind = details['rules'].index(x)
                print(f"Rule#"+ str(x) + " - " + str(details['result'][ind]) + " : " + details['acts'][ind][1])
            #todo response using another library
            #todo server to send this responce, hehe
        else:
            print(f"[warning] unknown operation!\n{details}")                
        if delivery_required:
            proceed_to_deliver(id, details)
    except Exception as e:
        print(f"[error] failed to handle request: {e}")
    


def consumer_job(args, config):
    # Create Consumer instance
    connector_consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(connector_consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            connector_consumer.assign(partitions)

    # Subscribe to topic
    topic = "connector"
    connector_consumer.subscribe([topic], on_assign=reset_offset)

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = connector_consumer.poll(1.0)
            if msg is None:
                pass
            elif msg.error():
                print(f"[error] {msg.error()}")
            else:
                try:
                    id = msg.key().decode('utf-8')
                    details_str = msg.value().decode('utf-8')
                    handle_event(id, details_str)
                except Exception as e:
                    print(
                        f"[error] Malformed event received from topic {topic}: {msg.value()}. {e}")
    except KeyboardInterrupt:
        pass
    finally:
        connector_consumer.close()


def start_consumer(args, config):
    threading.Thread(target=lambda: consumer_job(args, config)).start()


if __name__ == '__main__':
    start_consumer(None)
