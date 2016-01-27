Syscol Mesos Framework
======================

Installation
------------

Install go 1.4 (or higher) http://golang.org/doc/install

Install godep https://github.com/tools/godep

Clone and build the project

    # git clone https://github.com/elodina/syscol.git
    # cd syscol
    # godep restore
    # go build .
    # go build cli.go
    # go build executor.go

Usage
-----

Syscol framework ships with command-line utility to manage schedulers and executors:

    # ./cli help
    Usage:
        help: show this message
        scheduler: configure and start scheduler
        start: start syscol reporters
        stop: stop syscol reporters
        update: update configuration
        status: get current status of cluster
    More help you can get from ./cli <command> -h


Scheduler Configuration
-----------------------

The scheduler is configured through the command line.

    # ./cli scheduler <options>

Following options are available:

    -master="": Mesos Master addresses.
    -api="": Binding host:port for http/artifact server. Optional if SM_API env is set.
    -user="": Mesos user. Defaults to current system user.
    -log.level="info": Log level. trace|debug|info|warn|error|critical. Defaults to info.
    -framework.name="syscol": Framework name.
    -framework.role="*": Framework role.

Starting and Stopping a Server
------------------------------

    # ./cli start|stop <options>

Options available:

    -api="": Binding host:port for http/artifact server. Optional if SM_API env is set.

Updating Server Preferences
---------------------------

    # ./cli update <options>

Following options are available:

    -api="": Binding host:port for http/artifact server. Optional if SM_API env is set.
    -producer.properties="": Producer.properties file name.
    -topic="": Topic to produce data to.
    -transform="": Transofmation to apply to each metric. none|avro
    -schema.registry.url="": Avro Schema Registry url for transform=avro
    -reporting.

Quick start:
-----------

```
# export SM_API=http://master:6666
# ./cli scheduler --master master:5050
# ./cli update --producer.properties producer.properties --topic metrics
# ./cli start
```

By now you should see metrics from each Mesos slave getting produced to Kafka with reporting interval of 1 second (*TODO* configurable reporting interval)