PATH_PREFIX := $(CURDIR)

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
  kafka-topics --create --if-not-exists \
    --topic crypto \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1

all: clean build run-broker delay30s run delay60s test

delay30s:
	sleep 30

delay60s:
	sleep 60

sys-packages:
	sudo apt install -y docker-compose
	sudo apt install python3-pip -y
	sudo pip install pipenv

broker:
	docker-compose -f kafka/docker-compose.yaml up -d

permissions:
	chmod a+w $(PATH_PREFIX)/storage/db
	chmod a+w $(PATH_PREFIX)/bre/db
	chmod a+w $(PATH_PREFIX)/storage/db/storage.db
	chmod a+w $(PATH_PREFIX)/bre/db/bre.db
	chmod a+w $(PATH_PREFIX)/equipment/db/equipment.db
	chmod a+w $(PATH_PREFIX)/equipment/db/
	chmod u+x $(PATH_PREFIX)/bre/bre.py
	chmod u+x $(PATH_PREFIX)/connector/connector.py
	chmod u+x $(PATH_PREFIX)/document/document.py
	chmod u+x $(PATH_PREFIX)/equipment/equipment.py
	chmod u+x $(PATH_PREFIX)/mixer/mixer.py
	chmod u+x $(PATH_PREFIX)/monitor/monitor.py
	chmod u+x $(PATH_PREFIX)/reporter/reporter.py
	chmod u+x $(PATH_PREFIX)/storage/storage.py
	chmod u+x $(PATH_PREFIX)/storage/crypto.py

pipenv:
	pipenv install -r requirements.txt

prepare: sys-packages permissions pipenv build run-broker

build:
	docker-compose build

run-broker:
	docker-compose up -d zookeeper broker

run:
	docker-compose up -d

test:
	pytest -sv

logs:
	docker-compose logs -f --tail 100

clean:
	docker-compose down; pipenv --rm; rm -f Pipfile*; echo cleanup complete

