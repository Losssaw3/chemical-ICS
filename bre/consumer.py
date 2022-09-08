# implements Kafka topic consumer functionality

from datetime import datetime
import multiprocessing
import random
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
    
    try:
        delivery_required = False
        if details['operation'] == 'confirmation':
           #todo checking tules from db
            connection = create_connection('./db/bre.db')
            
            create_table = """
            CREATE TABLE IF NOT EXISTS bre (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule INTEGER,
            element TEXT NOT NULL,
            temperature INTEGER,
            operation TEXT NOT NULL
            );
            """
            # amount INTEGER,
            # amount_link_element TEXT,
            # amount_link_amount INTEGER
            execute_query(connection, create_table)

            #create_storage = """
            #INSERT INTO
            #bre (rule, element, temperature, operation)
            #VALUES
            #(1, 'list', 500, 'more'),
            #(3, 'balloon', 50, 'less')
            #"""
            #(2, 'cathalizator', 2, 0, 'A', 50),
            #execute_query(connection, create_storage)
            
            select_bre = "SELECT * from bre"
            selected_bre= execute_read_query(connection, select_bre)
            #todo refactore
            #todo normal rules (and rule 2 from task )checking block
            check = False
            rules=[]
            result=[]
            #print('start bre checking' + str(time.time()))
            for x in selected_bre:
                #if not selected_bre.index(x) == 1: 
                    for i in details['from']:
                        if i.find(x[2])>=0:
                            t = details['from'][details['from'].index(i)].find('!')+1
                            if x[4] == 'more':
                                if x[3] <= int(details['from'][details['from'].index(i)][t:]):
                                    rules.append(x[1])
                                    result.append(True)
                                else:
                                    rules.append(x[1])
                                    result.append(False)
                            if x[4] == 'less':
                                if x[3] > int(details['from'][details['from'].index(i)][t:]):
                                    rules.append(x[1])
                                    result.append(True)
                                else:
                                    rules.append(x[1])
                                    result.append(False)
                    for i in details['using']:
                        if i.find(x[2])>=0:
                            t = details['using'][details['using'].index(i)].find('!')+1
                            if x[4] == 'more':
                                if x[3] <= int(details['using'][details['using'].index(i)][t:]):
                                    rules.append(x[1])
                                    result.append(True)
                                else:
                                    rules.append(x[1])
                                    result.append(False)
                            if x[4] == 'less':
                                if x[3] > int(details['using'][details['using'].index(i)][t:]):
                                    rules.append(x[1])
                                    result.append(True)
                                else:
                                    rules.append(x[1])
                                    result.append(False)
            #print('end bre checking' + str(time.time()))

            details['rules'] = rules
            details['result'] = result
            if False in result:
                details['bool'] = False
                details['deliver_to'] = 'mixer'
                details['operation'] = 'confirmation'
                _requests_queue.put(details)
            else:
                details['bool'] = True
            #todo crutch with copy
            time.sleep(1)
            details['deliver_to'] = 'equipment'
            details['operation'] = 'confirmation'
            delivery_required = True
            
        else:
            print(f"[warning] unknown operation!\n{details}")                
        if delivery_required:
            proceed_to_deliver(id, details)
    except Exception as e:
        print(f"[error] failed to handle request: {e}")
    


def consumer_job(args, config, requests_queue: multiprocessing.Queue):
    # Create Consumer instance
    bre_consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(bre_consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            bre_consumer.assign(partitions)

    # Subscribe to topic
    topic = "bre"
    bre_consumer.subscribe([topic], on_assign=reset_offset)

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = bre_consumer.poll(1.0)
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
        bre_consumer.close()


def start_consumer(args, config, requests_queue):
    global _requests_queue
    _requests_queue = requests_queue
    threading.Thread(target=lambda: consumer_job(args, config, requests_queue)).start()


if __name__ == '__main__':
    start_consumer(None)
