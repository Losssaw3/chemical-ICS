## implements Kafka topic consumer functionality

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
import os


_requests_queue: multiprocessing.Queue = None

def create_connection(path):
    """Создает соединение с базой данных SQLite"""
    try:
        # Создаем директорию если ее нет
        os.makedirs(os.path.dirname(path), exist_ok=True)
        connection = sqlite3.connect(path)
        connection.execute("PRAGMA journal_mode=WAL")  # Включаем режим WAL для лучшей производительности
        return connection
    except sqlite3.Error as e:
        print(f"[DB ERROR] Failed to connect to database: {e}")
        return None

def execute_query(connection, query, params=None):
    """Выполняет запрос на изменение данных"""
    if connection is None:
        print("[DB ERROR] No database connection")
        return False
        
    cursor = connection.cursor()
    try:
        cursor.execute(query, params or ())
        connection.commit()
        return True
    except sqlite3.Error as e:
        print(f"[DB ERROR] Query failed: {e}\nQuery: {query}")
        return False

def execute_read_query(connection, query, params=None):
    """Выполняет запрос на чтение данных"""
    if connection is None:
        print("[DB ERROR] No database connection")
        return None
        
    cursor = connection.cursor()
    try:
        cursor.execute(query, params or ())
        return cursor.fetchall()
    except sqlite3.Error as e:
        print(f"[DB ERROR] Read query failed: {e}\nQuery: {query}")
        return None

def debug_rules_comparison(details: dict):
    """Визуализирует сравнение правил из БД с входными значениями"""
    connection = create_connection('./db/bre.db')
    if not connection:
        print("[error] Не удалось подключиться к БД")
        return
    
    try:
        # Получаем все правила из БД
        rules = execute_read_query(connection, "SELECT * FROM bre")
        if not rules:
            print("В базе нет правил")
            return
        
        print(f"Анализируем данные: from={details.get('from', [])}, using={details.get('using', [])}\n")
        
        for rule in rules:
            rule_id, rule_num, element, temp, operation = rule
            print(f"Правило #{rule_num} (id:{rule_id}): {element} {operation} {temp}")
            
            # Проверяем в разделе 'from'
            for item in details.get('from', []):
                if element in item:
                    try:
                        value = int(item.split('!')[1])
                        result = value >= temp if operation == 'more' else value < temp
                        print(f"  FROM {item}: {value} {operation} {temp}")
                    except (IndexError, ValueError):
                        print(f"  FROM {item}: Некорректный формат")
            
            # Проверяем в разделе 'using'
            for item in details.get('using', []):
                if element in item:
                    try:
                        value = int(item.split('!')[1])
                        result = value >= temp if operation == 'more' else value < temp
                        print(f"  USING {item}: {value} {operation} {temp}")
                    except (IndexError, ValueError):
                        print(f"  USING {item}: Некорректный формат")
            
            print("-" * 50)
    
    finally:
        connection.close()

def handle_event(id, details_str):
    """Обрабатывает событие из Kafka"""
    try:
        details = json.loads(details_str)
        debug_rules_comparison(details)
        print(f"[info] handling event {id}, {details['source']}->{details['deliver_to']}: {details['operation']}")
        
        if details['operation'] == 'confirmation':
            # Подключаемся к базе данных
            connection = create_connection('./db/bre.db')
            if connection is None:
                print("[error] Failed to connect to database")
                return
            
            try:
                # Создаем таблицу если она не существует
                create_table = """
                CREATE TABLE IF NOT EXISTS bre (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rule INTEGER,
                    element TEXT NOT NULL,
                    temperature INTEGER,
                    operation TEXT NOT NULL
                );
                """
                if not execute_query(connection, create_table):
                    print("[error] Failed to create table")
                    return
                
                # Получаем все правила из базы данных
                select_bre = "SELECT * FROM bre"
                rules = execute_read_query(connection, select_bre)
                if rules is None:
                    print("[error] Failed to read rules from database")
                    return
                
                check_results = []
                matched_rules = []
                
                # Проверяем каждое правило
                for rule in rules:
                    rule_id, _, element, temp, operation = rule
                    
                    # Проверяем в разделе 'from'
                    for item in details.get('from', []):
                        if element in item:
                            try:
                                value = int(item.split('!')[1])
                                if operation == 'more':
                                    check_results.append(value >= temp)
                                else:  # operation == 'less'
                                    check_results.append(value < temp)
                                matched_rules.append(rule_id)
                            except (IndexError, ValueError) as e:
                                print(f"[warning] Invalid format in 'from' item: {item}. Error: {e}")
                                continue
                    
                    # Проверяем в разделе 'using'
                    for item in details.get('using', []):
                        if element in item:
                            try:
                                value = int(item.split('!')[1])
                                if operation == 'more':
                                    check_results.append(value >= temp)
                                else:  # operation == 'less'
                                    check_results.append(value < temp)
                                matched_rules.append(rule_id)
                            except (IndexError, ValueError) as e:
                                print(f"[warning] Invalid format in 'using' item: {item}. Error: {e}")
                                continue
                
                # Формируем результат
                details['rules'] = matched_rules
                details['result'] = check_results
                details['bool'] = all(check_results) if check_results else False
                
                # Определяем куда отправлять результат
                if not details['bool']:
                    details['deliver_to'] = 'mixer'
                else:
                    details['deliver_to'] = 'equipment'
                
                details['operation'] = 'confirmation'
                proceed_to_deliver(id, details)
                
                time.sleep(1)
                
            except Exception as e:
                print(f"[error] Database operation failed: {e}")
            finally:
                connection.close()
                
    except json.JSONDecodeError as e:
        print(f"[error] Invalid JSON format in event {id}: {e}")
    except KeyError as e:
        print(f"[error] Missing required field in event {id}: {e}")
    except Exception as e:
        print(f"[critical] Unexpected error in handle_event {id}: {e}")

def consumer_job(args, config, requests_queue: multiprocessing.Queue):
    """Основная функция потребителя Kafka"""
    bre_consumer = Consumer(config)

    def reset_offset(bre_consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            bre_consumer.assign(partitions)

    topic = "bre"
    bre_consumer.subscribe([topic], on_assign=reset_offset)

    try:
        while True:
            msg = bre_consumer.poll(1.0)
            if msg is None:
                continue
            elif msg.error():
                print(f"[error] {msg.error()}")
            else:
                try:
                    id = msg.key().decode('utf-8')
                    details_str = msg.value().decode('utf-8')
                    handle_event(id, details_str)
                except Exception as e:
                    print(f"[error] Malformed event received from topic {topic}: {msg.value()}. {e}")
    except KeyboardInterrupt:
        print("[info] Consumer interrupted")
    finally:
        bre_consumer.close()

def start_consumer(args, config, requests_queue):
    global _requests_queue
    _requests_queue = requests_queue
    threading.Thread(target=lambda: consumer_job(args, config, requests_queue)).start()


if __name__ == '__main__':
    start_consumer(None)
