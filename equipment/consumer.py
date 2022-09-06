# implements Kafka topic consumer functionality

from datetime import datetime
import multiprocessing
from random import randint
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

def mixing(id, details):
    time.sleep(randint(0,10))
    details['bool'] = True
    details['deliver_to'] = 'mixer'
    details['operation'] = 'operation_status'
    proceed_to_deliver(id, details)
        

def handle_event(id, details_str):
    details = json.loads(details_str)
    print(f"[info] handling event {id}, {details['source']}->{details['deliver_to']}: {details['operation']}")
    try:
        delivery_required = False
        if details['operation'] == 'ask_equipment':
            
            connection = create_connection('./db/equipmnet.db')
            # create_table = """
            # CREATE TABLE IF NOT EXISTS equipment (
            # id INTEGER PRIMARY KEY AUTOINCREMENT,
            # name TEXT NOT NULL,
            # number INTEGER,
            # status BOOL
            # );
            # """
            # execute_query(connection, create_table)

            # create_equipment = """
            # INSERT INTO
            # equipment (name, number, status)
            # VALUES
            # ('list', 11, TRUE),
            # ('list', 22, TRUE),
            # ('list', 33, TRUE),
            # ('balloon', 11, FALSE),
            # ('balloon', 12, FALSE),
            # ('balloon', 13, FALSE),
            # ('balloon', 14, FALSE)
            # """
            # execute_query(connection, create_equipment)

            # select_equipment = "SELECT * from equipment"
            # selected_equipment = execute_read_query(connection, select_equipment)
            # print(selected_equipment)

            # update_status = """
            #         UPDATE
            #         equipment
            #         SET
            #         status = TRUE
            #         """
            # execute_query(connection, update_status)

            for x in details['from']:
                if not x == 'storage':
                    select_equipment = "SELECT * from equipment WHERE (name = '%s') and (status = TRUE) LIMIT 1" % x
                    selected_equipment = execute_read_query(connection, select_equipment)
                    details['from'][details['from'].index(x)] += '#'+str(selected_equipment[0][2])
                    eq_id = selected_equipment[0][0]
                    update_status = """
                    UPDATE
                    equipment
                    SET
                    status = FALSE
                    WHERE
                    id = %s
                    """ % eq_id
                    execute_query(connection, update_status)

            for x in details['using']:
                select_equipment = "SELECT * from equipment WHERE (name = '%s') and (status = TRUE) LIMIT 1" % x
                selected_equipment = execute_read_query(connection, select_equipment)
                eq_id = selected_equipment[0][0]
                details['using'][details['using'].index(x)] += '#'+str(selected_equipment[0][2])
                update_status = """
                UPDATE
                equipment
                SET
                status = FALSE
                WHERE
                id = %s
                """ % eq_id
                execute_query(connection, update_status)   
            
            connection.close()
            details['deliver_to'] = 'mixer'
            details['operation'] = 'list_equipment'
            delivery_required = True

        elif details['operation'] == 'equipment_status_req':
            #read from details list of equip and random it's status
            #you don't have to comment this fantastic idea (with # and !), i know about it's quality :))
            for x in details['from']:
                if not x == 'storage':
                    details['from'][details['from'].index(x)] += '!'+str(randint(48,51))
            for x in details['using']:
                details['using'][details['using'].index(x)] += '!'+str(randint(499,503))

            details['deliver_to'] = 'mixer'
            details['operation'] = 'equipment_status'
            delivery_required = True


        elif details['operation'] == 'confirmation':
            if details['bool']:
                threading.Thread(target=lambda: mixing(id, details)).start()
            #todo connect with mixing thread ending if it's exist
            connection = create_connection('./db/equipmnet.db')
            for x in details['from']:
                if not x == 'storage':
                    number = str(x[x.find('#')+1:x.find('!')])
                    name = str(x[:x.find('#')])
                    select_equipment = "SELECT * from equipment WHERE (number = '%s') and (name = '%s') LIMIT 1" % (number,name)
                    selected_equipment = execute_read_query(connection, select_equipment)
                    eq_id = selected_equipment[0][0]
                    update_status = """
                    UPDATE
                    equipment
                    SET
                    status = TRUE
                    WHERE
                    id = %s
                    """ % eq_id
                    execute_query(connection, update_status)
            for x in details['using']:
                number = str(x[x.find('#')+1:x.find('!')])
                name = str(x[:x.find('#')])
                select_equipment = "SELECT * from equipment WHERE (number = '%s') and (name = '%s') LIMIT 1" % (number, name)
                selected_equipment = execute_read_query(connection, select_equipment)
                eq_id = selected_equipment[0][0]
                update_status = """
                UPDATE
                equipment
                SET
                status = TRUE
                WHERE
                id = %s
                """ % eq_id
                execute_query(connection, update_status)     
            connection.close()
            delivery_required = False
        else:
            print(f"[warning] unknown operation!\n{details}")                
        if delivery_required:
            proceed_to_deliver(id, details)
    except Exception as e:
        print(f"[error] failed to handle request: {e}")
    

def consumer_job(args, config):
    # Create Consumer instance
    equipment_consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(equipment_consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            equipment_consumer.assign(partitions)

    # Subscribe to topic
    topic = "equipment"
    equipment_consumer.subscribe([topic], on_assign=reset_offset)

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = equipment_consumer.poll(1.0)
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
        equipment_consumer.close()


def start_consumer(args, config):
    threading.Thread(target=lambda: consumer_job(args, config)).start()


if __name__ == '__main__':
    start_consumer(None)
