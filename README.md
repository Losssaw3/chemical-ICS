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
- _make create_topics_

execute in VS Code terminal window either
- _make run_
- or _docker-compose up_


#### Troubleshooting

- if kafka or zookeeper containers don't start, make sure you don't have containers with the same name. If you do, remove the old containers and run the demo again.
- also broken can be down after start - use _up_ command to reload it and everything will be okay.
