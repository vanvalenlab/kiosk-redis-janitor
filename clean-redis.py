# Copyright 2016-2019 The Van Valen Lab at the California Institute of
# Technology (Caltech), with support from the Paul Allen Family Foundation,
# Google, & National Institutes of Health (NIH) under Grant U24CA224309-01.
# All rights reserved.
#
# Licensed under a modified Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.github.com/vanvalenlab/kiosk-redis-janitor/LICENSE
#
# The Work provided may be used for non-commercial academic purposes only.
# For any other use of the Work, including commercial use, please contact:
# vanvalenlab@gmail.com
#
# Neither the name of Caltech nor the names of its contributors may be used
# to endorse or promote products derived from this software without specific
# prior written permission.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ============================================================================
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import logging.handlers
import sys
import time
import traceback

import decouple

import redis_janitor


def initialize_logger(debug_mode=True):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '[%(asctime)s]:[%(levelname)s]:[%(name)s]: %(message)s')
    console = logging.StreamHandler(stream=sys.stdout)
    console.setFormatter(formatter)

    fh = logging.handlers.RotatingFileHandler(
        filename='redis-janitor.log',
        maxBytes=10000000,
        backupCount=10)
    fh.setFormatter(formatter)

    if debug_mode:
        console.setLevel(logging.DEBUG)
    else:
        console.setLevel(logging.INFO)
    fh.setLevel(logging.DEBUG)

    logger.addHandler(console)
    logger.addHandler(fh)
    logging.getLogger('kubernetes.client.rest').setLevel(logging.INFO)


if __name__ == '__main__':
    initialize_logger(decouple.config('DEBUG', default=True, cast=bool))

    INTERVAL = decouple.config('INTERVAL', default=20, cast=int)
    QUEUE = decouple.config('QUEUE', default='predict')
    STALE_TIME = decouple.config('STALE_TIME', default='600', cast=int)
    QUEUE_DELIMITER = decouple.config('QUEUE_DELIMITER', default=',')

    _logger = logging.getLogger(__file__)

    REDIS = redis_janitor.redis.RedisClient(
        decouple.config('REDIS_HOST', default='redis-master'),
        decouple.config('REDIS_PORT', default=6379, cast=int))

    janitor = redis_janitor.RedisJanitor(
        redis_client=REDIS,
        queue=QUEUE,
        queue_delimiter=QUEUE_DELIMITER,
        stale_time=STALE_TIME)

    base_queues = ' and '.join('`%s`' % q for q in janitor.queues)
    queues = ' and '.join('`%s:*`' % q for q in janitor.processing_queues)
    _logger.info('Janitor initialized. Cleaning queues %s and %s every %ss.',
                 base_queues, queues, INTERVAL)

    while True:
        try:
            janitor.clean()
            time.sleep(INTERVAL)
        except Exception as err:  # pylint: disable=broad-except
            _logger.critical('Fatal Error: %s: %s', type(err).__name__, err)
            _logger.critical(traceback.format_exc())
            sys.exit(1)
