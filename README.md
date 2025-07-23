# Stress Tester for OrientDB

A Docker based stress testing tool for [OrientDB](https://github.com/orientechnologies/orientdb).

This tool was primarily designed for OrientDB developers to observe the behaviour of the a distributed (multi-node) database under various stress conditions (e.g. restarting, crashing). 

## Version Compatibility

This tool currently only works on patched versions of OrientDB that incorporate changes to the `HA STATUS` API that allow HA database state to be examined remotely.

At time of writing these patches can be found at https://github.com/indexity-io/orientdb/tree/3.1/ha_db_stability

In addition, the tool currently utilises a patch that artificially expands the startup time of the distributed plugin to expose issues caused during that window to simulate real world conditions where the OrientDB nodes are on remote nodes with longer TCP latencies.

For OrientDB Community, ...
For OrientDB Enterprise Edition, ...

The tool has been tested against OrientDB 3.1 and 3.2. Other versions are unlikely to work.

# Usage

## General Structure

This tool essentially does the following:

- starts a cluster of OrientDB nodes and verifies it has reached operational status
- creates test database, and installs a test schema and test records (if workload is enabled)
- if enabled, starts a query/update workload on the database
- periodically performs actions to disturb/stress the cluster
- after each disturbance, verifies that the cluster returns to operation

During the scenario, the OrientDB logs and workload actions are monitored for errors, and all errors found are classified and logged.

Each scenario execution is recorded in a numbered folder under the `./scenarios` folder. The scenario recording includes:

- log files of each of the OrientDB nodes (which are live updated during scenario execution), named `docker-<nodename>.log`
- a `log.txt` file that reproduces the log transcript from the console (this is all the logging at the `INFO` or higher level).
- a `log-debug.txt` file that contains all logging, including `DEBUG` messages.


On scenario completion, additional files are created:

- `completed` marker file is created.
- if the scenario failed, a `failed` marker file is created.
- an `errors` file is created that summarises all the errors encountered during the scenario, including the phase, source and log location of each error.
- `errors_UNKNOWN` and `errors_KNOWN` files are created if any unknown or known errors are encountered, summarising the phase and log location of each error.
- an `orientdb-backup.tar.gz` archive is created, containing the data and backup directories of each of the OrientDB nodes.

## Error Classification

During scenario execution, errors encountered in OrientDB logs and workload responses are classified into three classifications:

- `SUPPRESSED` - these errors are noted, but ignored (i.e. they're spurious errors/warnings that do not affect operation).
- `KNOWN` - these errors have had classification patterns added for them, but are still considered significant.
- `UNKNOWN` - these errors have not had classification patterns registered. Some effort is made to extract an error type from the information encountered.

Classification patterns for `SUPPRESSED` or `KNOWN` errors can be added by modifying the relevant error classifier in the source code - see `OrientDBErrorClassifier` and `OrientDBRESTErrorClassifier` in the code for examples.

## External Dependencies

 Docker Compose is used to manage the OrientDB server instances. A recent version of Docker that supports the `docker compose` sub-command should work.

## Installation

Currently only running from source is supported. See the development instructions for how to set up and run.

## Running Scenarios

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
                        Kill server uncleanly in alternate stop/start scenarios (default: False)
```

# Supported Scenarios

## Basic Startup

`orientdb-stress run basic-startup`

Starts a cluster, wait for HA to stabilise, run workload for scenario length (if enabled), and then shut down.

## Random Restart

`orientdb-stress run random-restart`

Restarts a random server node at intervals.
This tests the behaviour of the system when a node is restarted (perhaps with a small amount of downtime).

## Alternating Stop Start

`orientdb-stress run alternating-stop-start`

Stops and starts a random node, waiting for HA status to stabilise after each operation.
In comparison to the restart scenario, this scenario tests the behaviour of the system when nodes are taken down for maintenance for an extended period, and then restored to operation.

## Rolling Restart

`orientdb-stress run rolling-restart`

Sequentially restarts server nodes at intervals, validating HA status after each set of restarts.
This is a pathological scenario, designed to expose problems with distributed startup and shutdown when the cluster is highly unstable.

## Random Kill

`orientdb-stress run random-kill`

Kills a random server node at intervals.
This test is similar to the random restart scenario, but uncleanly kills the OrientDB nodes using `KILL` signals.
This tests the ability of the cluster to recover from node failures, as well as the ability to recover from unclean shutdown of data stores.

# Workloads

All of the scenarios can be run with a query/update workload by providing the `--enable_workload` arguments, and customised using various `--workload_*` arguments.

To apply a read-only workload, also specify `--workload_readonly`.

At present the only workload implemented is updates to a property with a unique index.

## Workload Validation

When workload is enabled, operation of the database to server query/update workloads will be verified at each verification phase:

 - for read-only workloads, records are queried.
 - for update workloads, records are queried, updated and queried again to check for successful update - this will detect failed updates, or lost updates.

# Developing

Install [Poetry 1.8+](https://python-poetry.org/).

`poetry install`
`poetry shell`

A Visual Studio Code workspace is also included.

A shim script is included in the project root, which allows running the module live without having to repeatedly run `poetry install`.

`stress.py` simply invokes the core command, and passes all command line arguments.