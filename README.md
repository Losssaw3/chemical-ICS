# chemical-ICS

## Disclaimer 

This is a demo project and shall not be used in production.
The code is distributed under MIT license (see the LICENSE file).

## Purpose

This is an example of simple chemical ICS hardening by using "Security monitor" pattern: all cross-servie requests go through the Monitor service.
The Monitor checks whether particular request is authorized and valid, then delivers it to destination service or drops it without further processing.

## Running the demo

There is the main options for running the demo:
- containerized (using docker containers)

There shall be docker-compose locally available - at least for running message broker (Kafka).

### Running complete demo in containerized mode
be sure that you have got directories db in equipments, bre, storage and documents directories - you should have enough rules to use them by docker - for examle, _chmod 777 ./db_ will help you.

if it's first use, use next commands one by one:
- _docker-compose build_
- _make prepare_
- _make run_
- _make create-topics_

execute in VS Code terminal window either
- _make run_
- or _docker-compose up_


#### Troubleshooting

- if kafka or zookeeper containers don't start, make sure you don't have containers with the same name. If you do, remove the old containers and run the demo again.
- also broken can be down after start - use _up_ command to reload it and everything will be okay.
- if kafka or zookeeper containers don't start, make sure you don't have containers with the same name. If you do, remove the old containers and run the demo again.
- if you use a package manager other than apt, and trying _make all_ you need manually execute commands from the script sys-packages, after clear sys-packages section. Then try _make all_ again
- if after _make all_ you see output like this: [Building wheel for confluent-kafka (pyproject.toml): started
  Building wheel for confluent-kafka (pyproject.toml): finished with status 'error'
Failed to build confluent-kafka] try to install librdkafka package. Then try _make all_ again
- if you don't accept confirmation from storage and see output like: Storage failed ... can't get confirmation.. This may be caused by a lack of reagents in the storage facility, to fix it you need refuel storage by editing storage/consumer.py 
you need to find the commented lines that use the function _update_or_insert_ uncomment it and edit as you want then reboot the project