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

execute in VS Code terminal window either
- _make run_
- or _docker-compose up_


#### Troubleshooting

- if kafka or zookeeper containers don't start, make sure you don't have containers with the same name. If you do, remove the old containers and run the demo again.
