# implements Kafka topic consumer functionality

from ast import operator
from datetime import datetime
import multiprocessing
import threading
import time
from confluent_kafka import Consumer, OFFSET_BEGINNING
import json
from producer import proceed_to_deliver
import base64


_requests_queue: multiprocessing.Queue = None

def decrypt(details):
    details['deliver_to'] = 'mixer'
    print("decrypt message from connector... , checking sign ...")

def encrypt(details):
    details['deliver_to'] = 'reporter'
    print("encrypt critical data to encrypt critical data to make request to DB")

def encrypt_n_sign(details):
    details['deliver_to'] = 'connector'
    print("encrypt report... and send it to connector")

def handle_event(id, details_str):
    details = json.loads(details_str)
    print(f"[info] handling event {id}, {details['source']}->{details['deliver_to']}: {details['operation']}")
    try:
        if details['operation'] == 'ordering':
            decrypt(details)
            proceed_to_deliver(details["id"], details)
        
        if details['operation'] == 'operation_status':
            encrypt(details)
            proceed_to_deliver(details["id"] , details)

        if details['operation'] == 'acts':
            encrypt_n_sign(details)
            proceed_to_deliver(details["id"] , details)
        
    except Exception as e:
        print(f"[error] failed to handle request: {e}")
    


def consumer_job(args, config):
    # Create Consumer instance
    reporter_consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(reporter_consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            reporter_consumer.assign(partitions)

    # Subscribe to topic
    topic = "crypto"
    reporter_consumer.subscribe([topic], on_assign=reset_offset)

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = reporter_consumer.poll(1.0)
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
        reporter_consumer.close()


def start_consumer(args, config):
    threading.Thread(target=lambda: consumer_job(args, config)).start()


if __name__ == '__main__':
    start_consumer(None)
