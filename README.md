# Stress Tester for OrientDB

A Docker based stress testing tool for [OrientDB](https://github.com/orientechnologies/orientdb).

This tool was primarily designed for OrientDB developers to observe the behaviour of the a distributed (muli-node) database under various stress conditions (e.g. restarting, crashing.

## Version Compatibility

This tool currently only works on patched versions of OrientDB that incorporate changes to the `HA STATUS` API that allow HA database state to be examined remotely.

For OrientDB Community, ...
For OrientDB Enterprise Edition, ...

The tool has been tested against OrientDB 3.1 and 3.2. Other versions are unlikely to work.

## Usage

### External Dependencies

 Docker Compose is used to manage the OrientDB server instances. A recent version of Docker that supports the `docker compose` sub-command should work.

### Installation

Currently only running from source is supported.

### Running Scenarios

`orientdb-stress list` will list all available scenarios.

`orientdb-stress run --help` will show available configuration options, including defaults.

```
usage: orientdb-stress run [-h] [-c SCENARIO_COUNT] [--scenario_length SCENARIO_LENGTH] [--dead_time DEAD_TIME] [--restart_interval RESTART_INTERVAL] [--enable_workload] [--workload_threads WORKLOAD_THREADS] [--workload_rate WORKLOAD_RATE]
                           [--workload_record_count WORKLOAD_RECORD_COUNT] [--workload_readonly] [--workload_validation_readonly] [--alternating_reset_server] [--alternating_kill_server]
                           scenario_name

positional arguments:
  scenario_name         Name of the scenario to execute

optional arguments:
  -h, --help            show this help message and exit
  -c SCENARIO_COUNT, --scenario_count SCENARIO_COUNT
                        Number of times to run the scenario (default: None)
  --scenario_length SCENARIO_LENGTH
                        Duration of the scenario in seconds (default: 60)
  --dead_time DEAD_TIME
                        Fractional number of seconds that a node stays dead during a restart (default: 0)
  --restart_interval RESTART_INTERVAL
                        Fractional number of seconds between node restarts in restart scenarios (default: 10)
  --enable_workload     Enables client workload during scenario (default: False)
  --workload_threads WORKLOAD_THREADS
                        Number of workload threads (default: 1)
  --workload_rate WORKLOAD_RATE
                        Rate to run workload at in fractional operations/second. (default: 10)
  --workload_record_count WORKLOAD_RECORD_COUNT
                        Number of records to use in query workload (default: 100)
  --workload_readonly   Perform only read operations in background workload (default: False)
  --workload_validation_readonly
                        Perform only read operations in validation workload (default: False)
  --alternating_reset_server
                        Reset data directory of stopped node in alternate stop/start scenarios (default: False)
  --alternating_kill_server
                        Kill server unclearnly in alternate stop/start scenarios (default: False)
```

## Supported Scenarios

### Basic Startup

`orientdb-stress run basic-startup`

Starts a cluster, wait for HA to stabilise, run workload for scenario length (if enabled), and then shut down.

### Random Restart

`orientdb-stress run random-restart`

Restarts a random server node at intervals.
This tests the behaviour of the system when a node is restarted (perhaps with a small amount of downtime).

### Alternating Stop Start

`orientdb-stress run alternating-stop-start`

Stops and starts a random node, waiting for HA status to stabilise after each operation.
In comparison to the restart scenario, this scenario tests the behaviour of the system when nodes are taken down for maintenance for an extended period, and then restored to operation.

### Rolling Restart

`orientdb-stress run rolling-restart`

Sequentially restarts server nodes at intervals, validating HA status after each set of restarts.
This is a pathological scenario, designed to expose problems with distributed startup and shutdown when the cluster is highly unstable.

### Random Kill

`orientdb-stress run random-kill`

Kills a random server node at intervals.
This test is similar to the random restart scenario, but uncleanly kills the OrientDB nodes using `KILL` signals.
This tests the ability of the cluster to recover from node failures, as well as the ability to recover from unclean shutdown of data stores.

## Developing

Install [Poetry 1.2+](https://python-poetry.org/), at least 

`poetry install`
`poetry shell`

A Visual Studio Code workspace is also included.

A shim script is included in the project root, which allows running the module live without having to repeatedly run `poetry install`.

`stress.py` simply invokes the core command, and passes all command line arguments.