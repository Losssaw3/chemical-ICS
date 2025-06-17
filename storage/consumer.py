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
import os

_requests_queue: multiprocessing.Queue = None

def create_connection(path):
    """Создает соединение с базой данных с проверкой прав доступа"""
    # Создаем папку для БД, если её нет
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    connection = None
    try:
        # Явно устанавливаем режим чтения/записи
        connection = sqlite3.connect(path)
        connection.execute("PRAGMA journal_mode=WAL") 
        return connection
    except sqlite3.Error as e:
        print(f"[DB ERROR] Failed to connect to database: {e}")
        return None

def execute_query(connection, query, params=None):
    """Выполняет запрос на изменение данных с обработкой ошибок"""
    if connection is None:
        print("[DB ERROR] No database connection")
        return False
        
    cursor = connection.cursor()
    try:
        cursor.execute(query, params or ())
        connection.commit()
        return True
    except sqlite3.Error as e:
        print(f"[DB ERROR] Query failed: {e}\nQuery: {query}\nParams: {params}")
        return False

def execute_read_query(connection, query, params=None):
    """Выполняет запрос на чтение данных с обработкой ошибок"""
    if connection is None:
        print("[DB ERROR] No database connection")
        return None
        
    cursor = connection.cursor()
    try:
        cursor.execute(query, params or ())
        return cursor.fetchall()
    except sqlite3.Error as e:
        print(f"[DB ERROR] Read query failed: {e}\nQuery: {query}\nParams: {params}")
        return None

def update_or_insert(connection, name, amount):
    """Обновляет существующий компонент или создает новый"""
    try:
        cursor = connection.cursor()
        
        cursor.execute(
            "UPDATE storage SET amount = amount + ? WHERE name = ?",
            (amount, name)
        )
        
        if cursor.rowcount == 0:
            cursor.execute(
                "INSERT INTO storage (name, amount) VALUES (?, ?)",
                (name, amount)
            )
            
        connection.commit()
        return True
        
    except sqlite3.Error as e:
        connection.rollback()
        print(f"[Ошибка] {e}")
        return False

def handle_event(id, details_str):
    """Обрабатывает событие из Kafka с полной проверкой данных"""
    try:
        details = json.loads(details_str)
        print(f"[info] handling event {id}, {details['source']}->{details['deliver_to']}: {details['operation']}")
    except json.JSONDecodeError:
        print(f"[error] Invalid JSON format in event {id}")
        return
    except KeyError as e:
        print(f"[error] Missing required field in event {id}: {e}")
        return

    try:
        delivery_required = False
        connection = create_connection('./db/storage.db')
        if connection is None:
            print("[error] Cannot proceed without database connection")
            return
        #update_or_insert(connection, 'A', 1500)
        #update_or_insert(connection, 'B', 1000)
        #update_or_insert(connection, 'cathalizator', 1000)
        if 'operation' not in details:
            print("[error] Missing 'operation' field in event details")
            return

        if details['operation'] == 'storage_book':
            # Проверяем обязательные поля для операции storage_book
            if 'mix' not in details or 'amount' not in details:
                print("[error] Missing required fields: mix or amount")
                return
                
            if len(details['mix']) != len(details['amount']):
                print("[error] mix and amount arrays must have same length")
                return

            details['bool'] = True  # По умолчанию считаем операцию успешной
            # Add debug query
            debug_query = "SELECT name, amount FROM storage"
            print(execute_read_query(connection, debug_query))
            for i, x in enumerate(details['mix']):
                try:
                    amount = details['amount'][i]
                    
                    # Ищем доступный товар на складе
                    select_storage = """
                    SELECT id, amount, blocked_amount 
                    FROM storage 
                    WHERE name = ? AND amount >= ? 
                    LIMIT 1
                    """
                    selected_storage = execute_read_query(connection, select_storage, (x, amount))
                    
                    if not selected_storage:
                        print(f"[error] No available stock for {x} (need {amount})")
                        details['bool'] = False         
                    # Обновляем количество
                    
                    eq_id, curr_amount, curr_blocked = selected_storage[0]
                    update_status = """
                    UPDATE storage
                    SET amount = ?,
                    blocked_amount = ?
                    WHERE id = ?
                    """
                    if not execute_query(connection, update_status, 
                                      (curr_amount - amount, curr_blocked + amount, eq_id)):
                        details['bool'] = False
                        print("storage failed at substaction")
                        
                except Exception as e:
                    print(f"[error] failed to book {x} in storage: {e}")
                    details['bool'] = False

            details['deliver_to'] = 'mixer'
            details['operation'] = 'storage_status'
            delivery_required = True

        elif details['operation'] == 'decomission':
            if 'mix' not in details or 'amount' not in details:
                print("[error] Missing required fields for decomission: mix or amount")
                return
                
            for i, x in enumerate(details['mix']):
                try:
                    amount = details['amount'][i]
                    
                    # Получаем текущее состояние
                    select_storage = "SELECT id, blocked_amount FROM storage WHERE name = ? LIMIT 1"
                    selected_storage = execute_read_query(connection, select_storage, (x,))
                    
                    if not selected_storage:
                        print(f"[error] Item {x} not found in storage")
                        continue
                        
                    eq_id, curr_blocked = selected_storage[0]
                    new_blocked = curr_blocked - amount
                    
                    # Проверяем, чтобы не уйти в минус
                    if new_blocked < 0:
                        print(f"[warning] Negative blocked amount for {x}, setting to 0")
                        new_blocked = 0
                    
                    update_status = "UPDATE storage SET blocked_amount = ? WHERE id = ?"
                    execute_query(connection, update_status, (new_blocked, eq_id))
                    
                except Exception as e:
                    print(f"[error] failed to decomission {x}: {e}")

        elif details['operation'] == 'unblock':
            if 'mix' not in details or 'amount' not in details:
                print("[error] Missing required fields for unblock: mix or amount")
                return
                
            for i, x in enumerate(details['mix']):
                try:
                    amount = details['amount'][i]
                    
                    # Получаем текущее состояние
                    select_storage = "SELECT id, amount, blocked_amount FROM storage WHERE name = ? LIMIT 1"
                    selected_storage = execute_read_query(connection, select_storage, (x,))
                    
                    if not selected_storage:
                        print(f"[error] Item {x} not found in storage")
                        continue
                        
                    eq_id, curr_amount, curr_blocked = selected_storage[0]
                    new_amount = curr_amount + amount
                    new_blocked = curr_blocked - amount
                    
                    # Проверяем, чтобы не уйти в минус
                    if new_blocked < 0:
                        print(f"[warning] Negative blocked amount for {x}, setting to 0")
                        new_blocked = 0
                    
                    update_status = """
                    UPDATE storage 
                    SET amount = ?,
                        blocked_amount = ?
                    WHERE id = ?
                    """
                    execute_query(connection, update_status, (new_amount, new_blocked, eq_id))
                    
                except Exception as e:
                    print(f"[error] failed to unblock {x}: {e}")

        else:
            print(f"[warning] unknown operation: {details['operation']}")

    except Exception as e:
        print(f"[critical error] failed to handle request: {e}")
    finally:
        if connection:
            connection.close()
            
    if delivery_required:
        proceed_to_deliver(id, details)

def consumer_job(args, config):
    """Основная функция потребителя Kafka"""
    consumer = Consumer(config)

    def reset_offset(consumer, partitions):
        if args and args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    topic = "storage"
    consumer.subscribe([topic], on_assign=reset_offset)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[kafka error] {msg.error()}")
                continue
                
            try:
                id = msg.key().decode('utf-8')
                details_str = msg.value().decode('utf-8')
                handle_event(id, details_str)
            except Exception as e:
                print(f"[error] Failed to process message: {e}")

    except KeyboardInterrupt:
        print("[info] Consumer interrupted")
    finally:
        consumer.close()

def start_consumer(args, config):
    threading.Thread(target=lambda: consumer_job(args, config)).start()


if __name__ == '__main__':
    start_consumer(None)
