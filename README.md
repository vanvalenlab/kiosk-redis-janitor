# kiosk-redis-janitor

[![Build Status](https://travis-ci.com/vanvalenlab/kiosk-redis-janitor.svg?branch=master)](https://travis-ci.com/vanvalenlab/kiosk-redis-janitor)
[![Coverage Status](https://coveralls.io/repos/github/vanvalenlab/kiosk-redis-janitor/badge.svg?branch=master)](https://coveralls.io/github/vanvalenlab/kiosk-redis-janitor?branch=master)

The DeepCell Kiosk uses Redis to implement a [reliable queue](https://redis.io/commands/rpoplpush#pattern-reliable-queue). The Janitor watches the processing queues and moves any invalid processing items back to the work queue. This ensures that no work items end up in stranded processing queues, and all work is finished in a timely fashion.

## Configuration

The Janitor is configured using environment variables. Please find a table of all environment variables and their description below.

| Name | Description | Default Value |
| :---: | :---: | :---: |
| `INTERVAL` | How frequently the Janitor checks for stale items, in seconds. | `20` |
| `QUEUES` | A `QUEUE_DELIMITER` separated list of work queues to monitor. | `predict` |
| `QUEUE` | *Deprecated in favor of `QUEUES`*. | `predict` |
| `QUEUE_DELIMITER` | A string used to separate a list of queue names in `QUEUES`. | `,` |
| `REDIS_HOST` | The IP address or hostname of Redis. | `redis-master` |
| `REDIS_PORT` | The port of Redis. | `6379` |
| `STALE_TIME` | The time after which a job is "stale", in seconds.  | `600` |
