# implements Kafka topic consumer functionality

from datetime import datetime
import math
import multiprocessing
from random import randrange
import sqlite3
import threading
import time
from confluent_kafka import Consumer, OFFSET_BEGINNING
from flask import request
import json
import os
from producer import proceed_to_deliver


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

def database_exists(db_path):
    return os.path.exists(db_path)

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
    db_path = './db/document.db'
    print(f"[info] handling event {id}, {details['source']}->{details['deliver_to']}: {details['operation']}")
    try:
        delivery_required = False
        if details['operation'] == 'need_acts':
            if not database_exists(db_path):
                connection = create_connection(db_path)
                create_table = """
                CREATE TABLE IF NOT EXISTS documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                rule INTEGER
                );
                """
                execute_query(connection, create_table)

                create_users = """
                INSERT INTO
                documents (name, rule)
                VALUES
                ('Технология производства серной кислоты от 04.04.1985, п12', 1),
                ('Технология производства серной кислоты от 04.04.1985', 2),
                ('Приказ 1234 от 12.09.1967', 3)
                """
                execute_query(connection, create_users)
                connection.close()
            else:
                connection = create_connection(db_path)
                arr = ''
                for x in details['rules']:
                    arr+=str(x) + ','
                arr = arr[:-1]
                select_users = "SELECT * from documents WHERE rule IN ( %s )" % arr
                rules = execute_read_query(connection, select_users)
                connection.close()
                details['acts'] = rules
                details['operation'] = 'acts_req'
                details['deliver_to'] = 'reporter'
                delivery_required = True
        else:
            print(f"[warning] unknown operation!\n{details}")                
        if delivery_required:
            proceed_to_deliver(id, details)
    except Exception as e:
        print(f"[error] failed to handle request: {e}")
    


def consumer_job(args, config):
    # Create Consumer instance
    document_consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(document_consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            document_consumer.assign(partitions)

    # Subscribe to topic
    topic = "document"
    document_consumer.subscribe([topic], on_assign=reset_offset)

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = document_consumer.poll(1.0)
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
        document_consumer.close()


def start_consumer(args, config):
    threading.Thread(target=lambda: consumer_job(args, config)).start()


if __name__ == '__main__':
    start_consumer(None)
