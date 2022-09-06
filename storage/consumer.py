# implements Kafka topic consumer functionality

from datetime import datetime
import math
import multiprocessing
from pickle import TRUE
from random import randrange
import sqlite3
import threading
import time
from confluent_kafka import Consumer, OFFSET_BEGINNING
import json
from producer import proceed_to_deliver
import base64


_requests_queue: multiprocessing.Queue = None

def create_connection(path):
    connection = None
    try:
        connection = sqlite3.connect(path)
        #print("Connection to SQLite DB successful")
    except sqlite3.Error as e:
        print(f"The error '{e}' occurred")

    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        #print("Query executed successfully")
    except sqlite3.Error as e:
        print(f"The error '{e}' occurred")

def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except sqlite3.Error as e:
        print(f"The error '{e}' occurred")            

def handle_event(id, details_str):
    details = json.loads(details_str)
    print(f"[info] handling event {id}, {details['source']}->{details['deliver_to']}: {details['operation']}")
    global x_coord
    global y_coord
    try:
        delivery_required = False
        connection = create_connection('./db/storage.db')
        if details['operation'] == 'storage_book':
            
            # create_table = """
            # CREATE TABLE IF NOT EXISTS storage (
            # id INTEGER PRIMARY KEY AUTOINCREMENT,
            # name TEXT NOT NULL,
            # amount INTEGER,
            # blocked_amount INTEGER
            # );
            # """
            # execute_query(connection, create_table)

            # create_storage = """
            # INSERT INTO
            # storage (name, amount, blocked_amount)
            # VALUES
            # ('A', 300, 0),
            # ('B', 300, 0),
            # ('C', 300, 0),
            # ('cathalizator', 300, 0)
            # """
            # execute_query(connection, create_storage)

            # update_status = """
            #         UPDATE
            #         storage
            #         SET
            #         amount = 400,
            #         blocked_amount = 0
            #         WHERE
            #         id = 1
            #         """ 
            # execute_query(connection, update_status) 


            # select_storage = "SELECT * from storage"
            # selected_storage = execute_read_query(connection, select_storage)
            # print(selected_storage)
            
            for x in details['mix']:
                try:
                    amount = details['amount'][details['mix'].index(x)]
                    select_storage = "SELECT * from storage WHERE (name = '%s') and (amount >= %i) LIMIT 1" % (x, amount)
                    selected_storage = execute_read_query(connection, select_storage)
                    eq_id = selected_storage[0][0]
                    n_amount = selected_storage[0][2] - amount
                    n_b_amount = selected_storage[0][3] + amount
                    update_status = """
                    UPDATE
                    storage
                    SET
                    amount = %i,
                    blocked_amount = %i
                    WHERE
                    id = %s
                    """ % (n_amount, n_b_amount, eq_id)
                    execute_query(connection, update_status) 
                    details['bool'] = True
                except Exception as e:
                    print(f"[error] failed to book in storage: {e}")
                    details['bool'] = False
            details['deliver_to'] = 'mixer'
            details['operation'] = 'storage_status'
            delivery_required = True
        elif details['operation'] == 'decomission':
            for x in details['mix']:
                try:
                    amount = details['amount'][details['mix'].index(x)]
                    select_storage = "SELECT * from storage WHERE (name = '%s') LIMIT 1" % x
                    selected_storage = execute_read_query(connection, select_storage)
                    eq_id = selected_storage[0][0]
                    n_b_amount = selected_storage[0][3] - amount
                    update_status = """
                    UPDATE
                    storage
                    SET
                    blocked_amount = %i
                    WHERE
                    id = %s
                    """ % (n_b_amount, eq_id)
                    execute_query(connection, update_status)
                    print(f"[decomissiong] event {id} successfully ended")
                except Exception as e:
                    print(f"[error] failed to decomission in storage: {e}")
            delivery_required = False
        elif details['operation'] == 'unblock':
            for x in details['mix']:
                try:
                    amount = details['amount'][details['mix'].index(x)]
                    select_storage = "SELECT * from storage WHERE (name = '%s') LIMIT 1" % x
                    selected_storage = execute_read_query(connection, select_storage)
                    eq_id = selected_storage[0][0]
                    n_amount = selected_storage[0][2] + amount
                    n_b_amount = selected_storage[0][3] - amount
                    update_status = """
                    UPDATE
                    storage
                    SET
                    amount = %i,
                    blocked_amount = %i
                    WHERE
                    id = %s
                    """ % (n_amount, n_b_amount, eq_id)
                    execute_query(connection, update_status)
                    print(f"[unblock] event {id} successfully ended")
                except Exception as e:
                    print(f"[error] failed to unblock in storage: {e}")
            delivery_required = False
        else:
            print(f"[warning] unknown operation!\n{details}")                
        connection.close()
        if delivery_required:
            proceed_to_deliver(id, details)
    except Exception as e:
        print(f"[error] failed to handle request: {e}")
    


def consumer_job(args, config):
    # Create Consumer instance
    storage_consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(storage_consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            storage_consumer.assign(partitions)

    # Subscribe to topic
    topic = "storage"
    storage_consumer.subscribe([topic], on_assign=reset_offset)

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = storage_consumer.poll(1.0)
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
        storage_consumer.close()


def start_consumer(args, config):
    threading.Thread(target=lambda: consumer_job(args, config)).start()


if __name__ == '__main__':
    start_consumer(None)
