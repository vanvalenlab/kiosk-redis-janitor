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
"""Tests for Janitor Class"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import numpy as np
import redis

from redis_janitor import janitors


class DummyRedis(object):
    def __init__(self, prefix='predict', status='new', fail_tolerance=0):
        self.fail_count = 0
        self.fail_tolerance = fail_tolerance
        self.prefix = '/'.join(x for x in prefix.split('/') if x)
        self.status = status

    def keys(self):
        if self.fail_count < self.fail_tolerance:
            self.fail_count += 1
            raise redis.exceptions.ConnectionError('thrown on purpose')
        return [
            '{}_{}_{}'.format(self.prefix, self.status, 'x.tiff'),
            '{}_{}_{}'.format(self.prefix, 'other', 'x.zip'),
            '{}_{}_{}'.format('other', self.status, 'x.TIFF'),
            '{}_{}_{}'.format(self.prefix, self.status, 'x.ZIP'),
            '{}_{}_{}'.format(self.prefix, 'other', 'x.tiff'),
            '{}_{}_{}'.format('other', self.status, 'x.zip'),
        ]

    def scan_iter(self, match=None):
        if self.fail_count < self.fail_tolerance:
            self.fail_count += 1
            raise redis.exceptions.ConnectionError('thrown on purpose')

        keys = [
            '{}_{}_{}'.format(self.prefix, self.status, 'x.tiff'),
            '{}_{}_{}'.format(self.prefix, 'other', 'x.zip'),
            '{}_{}_{}'.format('other', self.status, 'x.TIFF'),
            '{}_{}_{}'.format(self.prefix, self.status, 'x.ZIP'),
            '{}_{}_{}'.format(self.prefix, 'other', 'x.tiff'),
            '{}_{}_{}'.format('other', self.status, 'x.zip'),
        ]
        if match:
            return (k for k in keys if k.startswith(match[:-1]))
        return (k for k in keys)

    def expected_keys(self, suffix=None):
        for k in self.keys():
            v = k.split('_')
            if v[0] == self.prefix:
                if v[1] == self.status:
                    if suffix:
                        if v[-1].lower().endswith(suffix):
                            yield k
                    else:
                        yield k

    def hmset(self, rhash, hvals):  # pylint: disable=W0613
        if self.fail_count < self.fail_tolerance:
            self.fail_count += 1
            raise redis.exceptions.ConnectionError('thrown on purpose')
        return hvals

    def hget(self, rhash, field):
        if self.fail_count < self.fail_tolerance:
            self.fail_count += 1
            raise redis.exceptions.ConnectionError('thrown on purpose')
        if field == 'status':
            return rhash.split('_')[1]
        elif field == 'identity_started':
            if 'good' in rhash:
                return 'good_pod'
            elif 'badhost' in rhash:
                return None
            else:
                return 'bad_pod'
        elif field == 'timestamp_last_status_update':
            if 'malformed' in rhash:
                return None
            if 'stale' in rhash:
                return time.time() - 400000
            return time.time()
        return None

    def hset(self, rhash, status, value):  # pylint: disable=W0613
        if self.fail_count < self.fail_tolerance:
            self.fail_count += 1
            raise redis.exceptions.ConnectionError('thrown on purpose')
        return {status: value}

    def hgetall(self, rhash):  # pylint: disable=W0613
        if self.fail_count < self.fail_tolerance:
            self.fail_count += 1
            raise redis.exceptions.ConnectionError('thrown on purpose')
        return {
            'model_name': 'model',
            'model_version': '0',
            'field': '61',
            'cuts': '0',
            'postprocess_function': '',
            'preprocess_function': '',
            'file_name': rhash.split('_')[-1],
            'input_file_name': rhash.split('_')[-1],
            'output_file_name': rhash.split('_')[-1]
        }

    def type(self, key):  # pylint: disable=W0613
        if self.fail_count < self.fail_tolerance:
            self.fail_count += 1
            raise redis.exceptions.ConnectionError('thrown on purpose')
        return 'hash'


class TestJanitor(object):

    def test__get_pod_string(self):
        redis_client = DummyRedis(fail_tolerance=2)
        janitor = janitors.RedisJanitor(redis_client, backoff=0.01)
        # TODO: test retries
        # test that the output was captured
        s = janitor._get_pod_string(['echo', 'OUTPUT_STRING'])
        assert s == 'OUTPUT_STRING'

    def test__make_kubectl_call(self):
        # test that a command will run without error
        redis_client = DummyRedis(fail_tolerance=2)
        janitor = janitors.RedisJanitor(redis_client, backoff=0.01)
        janitor._make_kubectl_call(['echo', 'OUTPUT_STRING'])

    def test_hgetall(self):
        redis_client = DummyRedis(fail_tolerance=2)
        janitor = janitors.RedisJanitor(redis_client, backoff=0.01)

        data = janitor.hgetall('redis_hash')
        assert data == redis_client.hgetall('redis_hash')
        assert janitor.redis_client.fail_count == redis_client.fail_tolerance

    def test__redis_type(self):
        redis_client = DummyRedis(fail_tolerance=2)
        janitor = janitors.RedisJanitor(redis_client, backoff=0.01)

        data = janitor._redis_type('random_key')
        assert data == redis_client.type('random_key')
        assert janitor.redis_client.fail_count == redis_client.fail_tolerance

    def test_hset(self):
        redis_client = DummyRedis(fail_tolerance=2)
        janitor = janitors.RedisJanitor(redis_client, backoff=0.01)
        janitor.hset('rhash', 'key', 'value')
        assert janitor.redis_client.fail_count == redis_client.fail_tolerance

    def test_hget(self):
        redis_client = DummyRedis(fail_tolerance=2)
        janitor = janitors.RedisJanitor(redis_client, backoff=0.01)
        data = janitor.hget('rhash_new', 'status')
        assert data == 'new'
        assert janitor.redis_client.fail_count == redis_client.fail_tolerance

    def test_scan_iter(self):
        prefix = 'predict'
        redis_client = DummyRedis(fail_tolerance=2, prefix=prefix)
        janitor = janitors.RedisJanitor(redis_client, backoff=0.01)
        data = janitor.scan_iter(match=prefix)
        keys = [k for k in data]
        expected = [k for k in redis_client.keys() if k.startswith(prefix)]
        assert janitor.redis_client.fail_count == redis_client.fail_tolerance
        np.testing.assert_array_equal(keys, expected)

    def test_triage(self):
        redis_client = DummyRedis(fail_tolerance=0)
        janitor = janitors.RedisJanitor(redis_client, backoff=0.01)

        janitor.kill_pod = lambda x: True

        good_pod = 'good_pod status Running'
        bad_pod = 'bad_pod status Failed'
        good_pod_failed = 'good_pod status Failed'

        # test end point statuses
        assert janitor.triage('goodkey_failed', bad_pod) is True
        assert janitor.triage('goodkey_new', bad_pod) is False
        assert janitor.triage('goodkey_done', good_pod) is False

        # test malformed key (no hostname value)
        assert janitor.triage('badhost_weirdstatus', bad_pod) is False

        # test pod not found
        assert janitor.triage('badkey_inprogress', bad_pod) is True

        # test in progress with status != Running
        assert janitor.triage('goodkey_inprogress', good_pod_failed) is True

        # test in progress with status = Running with stale update time
        assert janitor.triage('goodkeystale_inprogress', good_pod) is True

        # test in progress with status = Running with fresh update time
        assert janitor.triage('goodkey_inprogress', good_pod) is False

        # test no `timestamp_last_status_update`
        assert janitor.triage('goodmalformed_inprogress', good_pod) is False

    def test_triage_keys(self):
        redis_client = DummyRedis(fail_tolerance=0)
        janitor = janitors.RedisJanitor(redis_client, backoff=0.01)

        # monkey-patch kubectl commands
        janitor.kill_pod = lambda x: True
        janitor._get_pod_string = lambda x: 'good_pod status Running'

        # run triage_keys
        janitor.triage_keys()

#    import subprocess
#    def test__make_kubectl_call(self):
#        # since these tests won't be run in a kiosk, kubectl shouldn't be installed,
#        # which should raise a FileNotFoundError
#        parameter_list = ["kubectl","get","pods"]
#        _make_kubectl_call(parameter_list)
#        
#        while True:
#            try:
#                break
#            except subprocess.CalledProcessError as err:
#                # For some reason, we can't execute this command right now.
#                # Keep trying until we can.
#                self.logger.warning('Encountered %s: %s while executing with '
#                                    'parameters: %s. Retrying in %s seconds...',
#                                    parameter_list, type(err).__name__, err,
#                                    self.backoff)
#                time.sleep(self.backoff)
