# implements Kafka topic consumer functionality


from glob import glob
import multiprocessing
from operator import eq
import threading
import time
from confluent_kafka import Consumer, OFFSET_BEGINNING
import json
from producer import proceed_to_deliver


_requests_queue: multiprocessing.Queue = None
st_book = False
saved_details = {"":""}
flags_read_attemt = 3

def check_flags(details):
    global flags_read_attemt
    if flags_read_attemt>0:
        if st_book:
            details['deliver_to'] = 'bre'
            details['operation'] = 'confirmation'
            proceed_to_deliver(details['id'], details)
        else:
            print("Waiting book, trying again")
            #todo may be "storage_book" request should be here?
            time.sleep(2)
            flags_read_attemt-=1
            check_flags(details)
    else:
        print('Storage failed, confirmation request can\'t be requsted!')

def status_equipment(details):
    #todo : now it's a crutch, should be copy func
    time.sleep(1)
    details['operation'] = 'equipment_status_req'
    details['deliver_to'] = 'equipment'
    proceed_to_deliver(details['id'], details)

def book_storage(details):
    if not st_book:
        details['operation'] = 'storage_book'
        details['deliver_to'] = 'storage'
        print(details)
        proceed_to_deliver(details['id'], details)


def handle_event(id: str, details):
    details = json.loads(details)
    
    print(f"[info] handling event {id}, {details['source']}->{details['deliver_to']}: {details['operation']}")
    try:
        delivery_required = False
        global st_book 
        global _requests_queue 
        #receive new order, answer - equipment list asking
        if details['operation'] == 'ordering':
            global st_book 
            st_book = False
            details['operation'] = "ask_equipment"
            details['deliver_to'] = 'equipment'
            delivery_required = True

        #list of booked equipment, answer - booking storage and asking equipment status
        elif details['operation'] == 'list_equipment':
            book_storage(details)
            copy_details = details
            status_equipment(copy_details)
            delivery_required = False

        #equipment status, check that storage booked, answer - confirmation to bre
        elif details['operation'] == 'equipment_status':
            global saved_details
            saved_details = details
            global _requests_queue 
            global flags_read_attemt
            flags_read_attemt = 3
            check_flags(details)
            delivery_required = False
        
        #storage booked, change flag
        elif details['operation'] == 'storage_status':
            st_book = details['bool']
            delivery_required = False
        
        #mixing failed, unblocking storage and reporting
        elif details['operation'] == 'confirmation':

            #global _requests_queue
            details['operation'] = 'unblock'
            details['deliver_to'] = 'storage'
            #_requests_queue.put(details)
            proceed_to_deliver(details['id'], details)

            #todo : now it's a crutch, should be copy func
            time.sleep(1)
            details['operation'] = 'operation_status'
            details['deliver_to'] = 'reporter'
            #_requests_queue.put(details)
            delivery_required = True

        #successfull mixing, decomission blocked storage and reporting
        elif details['operation'] == 'operation_status':
            details['operation'] = 'decomission'
            details['deliver_to'] = 'storage'
            proceed_to_deliver(details['id'], details)
            
            #todo : now it's a crutch, should be copy func
            time.sleep(1)
            details['operation'] = 'operation_status'
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
    mixer_consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(mixer_consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            mixer_consumer.assign(partitions)

    # Subscribe to topic
    topic = "mixer"
    mixer_consumer.subscribe([topic], on_assign=reset_offset)

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = mixer_consumer.poll(1.0)
            if msg is None:
                pass
            elif msg.error():
                print(f"[error] {msg.error()}")
            else:
                try:
                    #print(msg.value())
                    id = msg.key().decode('utf-8')
                    #changed here
                    details = msg.value().decode('utf-8')
                    handle_event(id, details)
                except Exception as e:
                    print(
                        f"[error] malformed event received from topic {topic}: {msg.value()}. {e}")    
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        mixer_consumer.close()

def start_consumer(args, config):
    threading.Thread(target=lambda: consumer_job(args, config)).start()
    
if __name__ == '__main__':
    start_consumer(None)
