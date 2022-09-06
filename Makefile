PATH_PREFIX=~

create-topics:
	docker exec broker \
  kafka-topics --create --if-not-exists \
    --topic monitor \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1
	docker exec broker \
  kafka-topics --create --if-not-exists \
    --topic bre \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1
	docker exec broker \
  kafka-topics --create --if-not-exists \
    --topic document \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1
	docker exec broker \
  kafka-topics --create --if-not-exists \
    --topic equipment \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1
	docker exec broker \
  kafka-topics --create --if-not-exists \
    --topic mixer \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1
	docker exec broker \
  kafka-topics --create --if-not-exists \
    --topic reporter \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1
	docker exec broker \
  kafka-topics --create --if-not-exists \
    --topic storage \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1
  	docker exec broker \
  kafka-topics --create --if-not-exists \
    --topic connector \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1


sys-packages:
	# sudo apt install -y docker-compose
	sudo apt install python3-pip -y
	sudo pip install pipenv

broker:
	docker-compose -f kafka/docker-compose.yaml up -d

permissions:
	chmod u+x $(PATH_PREFIX)/chemical-ICS/bre/bre.py
	chmod u+x $(PATH_PREFIX)/chemical-ICS/connector/connector.py
	chmod u+x $(PATH_PREFIX)/chemical-ICS/document/document.py
	chmod u+x $(PATH_PREFIX)/chemical-ICS/equipment/equipment.py
	chmod u+x $(PATH_PREFIX)/chemical-ICS/mixer/mixer.py
	chmod u+x $(PATH_PREFIX)/chemical-ICS/monitor/monitor.py
	chmod u+x $(PATH_PREFIX)/chemical-ICS/reporter/reporter.py
	chmod u+x $(PATH_PREFIX)/chemical-ICS/storage/storage.py
	chmod 766 $(PATH_PREFIX)/chemical-ICS/document/db
	chmod 766 $(PATH_PREFIX)/chemical-ICS/storage/db
	chmod 766 $(PATH_PREFIX)/chemical-ICS/equiment/db
	chmod 766 $(PATH_PREFIX)/chemical-ICS/bre/db

pipenv:
	pipenv install -r requirements.txt

prepare: sys-packages permissions pipenv build run-broker

prepare-screen:
	# WSL specific preparation
	sudo /etc/init.d/screen-cleanup start


run-screen: broker run-bre-screen run-connector-screen run-document-screen run-equipment-screen run-mixer-screen run-monitor-screen run-reporter-screen run-storage-screen

build:
	docker-compose build

run-broker:
	docker-compose up -d zookeeper broker

run:
	docker-compose up -d

restart:
	docker-compose restart

stop-app:
	pkill flask

restart-app: stop-app run


run-monitor-screen:
	screen -dmS monitor bash -c "cd $(PATH_PREFIX)/chemical-ICS/; pipenv run ./monitor/monitor.py config.ini"

run-monitor:
	cd $(PATH_PREFIX)/chemical-ICS/; pipenv run ./monitor/monitor.py config.ini


run-bre:
	cd $(PATH_PREFIX)/chemical-ICS/; pipenv run bre/bre.py config.ini

run-bre-screen:
	screen -dmS bre bash -c "cd $(PATH_PREFIX)/chemical-ICS/; pipenv run bre/bre.py config.ini"

run-connector:
	cd $(PATH_PREFIX)/chemical-ICS/; pipenv run connector/connector.py config.ini

run-connector-screen:
	screen -dmS connector bash -c "cd $(PATH_PREFIX)/chemical-ICS/; pipenv run connector/connector.py config.ini"

run-document:
	cd $(PATH_PREFIX)/chemical-ICS/; pipenv run document/document.py config.ini

run-document-screen:
	screen -dmS document bash -c "cd $(PATH_PREFIX)/chemical-ICS/; pipenv run document/document.py config.ini"

run-equipment:
	cd $(PATH_PREFIX)/chemical-ICS/; pipenv run equipment/equipment.py config.ini

run-equipment-screen:
	screen -dmS equipment bash -c "cd $(PATH_PREFIX)/chemical-ICS/; pipenv run equipment/equipment.py config.ini"

run-mixer:
	cd $(PATH_PREFIX)/chemical-ICS/; pipenv run mixer/mixer.py config.ini

run-mixer-screen:
	screen -dmS mixer bash -c "cd $(PATH_PREFIX)/chemical-ICS/; pipenv run mixer/mixer.py config.ini"

run-reporter:
	cd $(PATH_PREFIX)/chemical-ICS/; pipenv run reporter/reporter.py config.ini

run-reporter-screen:
	screen -dmS reporter bash -c "cd $(PATH_PREFIX)/chemical-ICS/; pipenv run reporter/reporter.py config.ini"

run-storage:
	cd $(PATH_PREFIX)/chemical-ICS/; pipenv run storage/storage.py config.ini

run-storage-screen:
	screen -dmS storage bash -c "cd $(PATH_PREFIX)/chemical-ICS/; pipenv run storage/storage.py config.ini"

test:
	pytest -sv
