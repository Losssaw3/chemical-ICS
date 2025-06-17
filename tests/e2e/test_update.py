from time import sleep
import pytest
import subprocess
import json
import re
import requests
import sqlite3
from urllib.request import urlopen, Request
from uuid import uuid1


ORDER_URL = "http://localhost:6006/ordering"
DB_PATH = "storage/db/storage.db"

headers = {
    "Content-Type": "application/json",
    "auth": "very-secure-token"
}
payload = {
    "mix": [
        "A",
        "B",
        "cathalizator"
    ],
    "amount": [
        100,
        10,
        11
    ],
    "from": [
        "storage",
        "balloon",
        "storage"
    ],
    "using": [
        "list"
    ]  
}

@pytest.fixture
def get_pattern():
    return pattern
@pytest.fixture
def get_logs():
    def _get_logs(container_name):
        return subprocess.check_output(
            ['docker-compose', 'logs', '--no-color', container_name],
            text=True,
        )
    return _get_logs

@pytest.fixture
def create_order():
    response = requests.post(url = ORDER_URL,headers=headers,json=payload)
    return response

@pytest.fixture
def storage_db():
    connection = sqlite3.connect(DB_PATH)
    yield connection
    connection.close()

def clean_equipment_string(s):
    return re.sub(r'[#!].*', '', s)


def get_storage_status(connection):
    debug_query = "SELECT name, amount FROM storage ORDER BY name"
    cursor = connection.cursor()
    cursor.execute(debug_query)
    reagents = {
        'A': 0,
        'B': 0, 
        'C': 0,
        'cathalizator': 0
    }
    for name, amount in cursor.fetchall():
        if name in reagents:
            reagents[name] = amount
    return reagents



def test_order_transmission(create_order):
    response = create_order
    assert response.status_code == 200
 
def test_reagent_usage(storage_db, get_logs):
    reagents_before = get_storage_status(storage_db)
    """Checking..."""
    sleep(40)
    reagents_after = get_storage_status(storage_db)
    logs = get_logs('bre')
    successful_result = "'result': [True, True]"
    if successful_result in logs:
        for i, reagent in enumerate(payload["mix"]):
            assert reagents_before[reagent] == reagents_after[reagent] + payload["amount"][i]
    else:
        for i, reagent in enumerate(payload["mix"]):
            assert reagents_before[reagent] == reagents_after[reagent]

def test_finish(get_logs):
    sleep(10)
    logs = get_logs('connector')
    cleaned_logs = clean_equipment_string(logs)
    s1 = (str(payload))[1:]
    successful_string = s1[:-1]
    assert successful_string in cleaned_logs