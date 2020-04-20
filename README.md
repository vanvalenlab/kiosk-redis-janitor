# ![DeepCell Kiosk Banner](https://raw.githubusercontent.com/vanvalenlab/kiosk-console/master/docs/images/DeepCell_Kiosk_Banner.png)

[![Build Status](https://travis-ci.com/vanvalenlab/kiosk-redis-janitor.svg?branch=master)](https://travis-ci.com/vanvalenlab/kiosk-redis-janitor)
[![Coverage Status](https://coveralls.io/repos/github/vanvalenlab/kiosk-redis-janitor/badge.svg?branch=master)](https://coveralls.io/github/vanvalenlab/kiosk-redis-janitor?branch=master)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](/LICENSE)

The DeepCell Kiosk uses Redis to implement a [reliable queue](https://redis.io/commands/rpoplpush#pattern-reliable-queue), which moves items from the work queue into processing queues to prevent any items from falling out of the queue. The `kiosk-redis-janitor` watches the processing queues and moves any invalid processing items back to the work queue. This ensures that no work items end up in stranded processing queues and all work is finished in a timely fashion.

This repository is part of the [DeepCell Kiosk](https://github.com/vanvalenlab/kiosk-console). More information about the Kiosk project is available through [Read the Docs](https://deepcell-kiosk.readthedocs.io/en/master) and our [FAQ](http://www.deepcell.org/faq) page.

## Configuration

The janitor is configured using environment variables. Please find a table of all environment variables and their descriptions below.

| Name | Description | Default Value |
| :--- | :--- | :--- |
| `INTERVAL` | How frequently the Janitor checks for stale items, in seconds. | `20` |
| `QUEUES` | A `QUEUE_DELIMITER` separated list of work queues to monitor. | `"predict"` |
| `QUEUE_DELIMITER` | A string used to separate a list of queue names in `QUEUES`. | `","` |
| `REDIS_HOST` | The IP address or hostname of Redis. | `"redis-master"` |
| `REDIS_PORT` | The port used to connect to Redis. | `6379` |
| `STALE_TIME` | The time after which a job is "stale", in seconds.  | `600` |

## Contribute

We welcome contributions to the [kiosk-console](https://github.com/vanvalenlab/kiosk-console) and its associated projects. If you are interested, please refer to our [Developer Documentation](https://deepcell-kiosk.readthedocs.io/en/master/DEVELOPER.html), [Code of Conduct](https://github.com/vanvalenlab/kiosk-console/blob/master/CODE_OF_CONDUCT.md) and [Contributing Guidelines](https://github.com/vanvalenlab/kiosk-console/blob/master/CONTRIBUTING.md).

## License

This software is license under a modified Apache-2.0 license. See [LICENSE](/LICENSE) for full  details.

## Copyright

Copyright © 2018-2020 [The Van Valen Lab](http://www.vanvalen.caltech.edu/) at the California Institute of Technology (Caltech), with support from the Paul Allen Family Foundation, Google, & National Institutes of Health (NIH) under Grant U24CA224309-01.
All rights reserved.
