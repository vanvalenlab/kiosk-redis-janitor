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

import redis

from redis_janitor import janitors


class Bunch(object):
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


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

    def scan_iter(self, match=None, count=None):
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
                return (time.time() - 400000) * 1000
            return time.time() * 1000
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


class DummyKubernetes(object):

    def delete_namespaced_pod(self, *args, **kwargs):
        return True

    def list_pod_for_all_namespaces(self, *args, **kwargs):
        return Bunch(items=[Bunch(status=Bunch(phase='Running'),
                                  metadata=Bunch(name='pod'))])


class TestJanitor(object):

    def test_hgetall(self):
        redis_client = DummyRedis(fail_tolerance=2)
        kube_client = DummyKubernetes()
        janitor = janitors.RedisJanitor(redis_client, kube_client, backoff=0.01)

        data = janitor.hgetall('redis_hash')
        assert data == redis_client.hgetall('redis_hash')
        assert janitor.redis_client.fail_count == redis_client.fail_tolerance

    def test__redis_type(self):
        redis_client = DummyRedis(fail_tolerance=2)
        kube_client = DummyKubernetes()
        janitor = janitors.RedisJanitor(redis_client, kube_client, backoff=0.01)

        data = janitor._redis_type('random_key')
        assert data == redis_client.type('random_key')
        assert janitor.redis_client.fail_count == redis_client.fail_tolerance

    def test_hset(self):
        redis_client = DummyRedis(fail_tolerance=2)
        kube_client = DummyKubernetes()
        janitor = janitors.RedisJanitor(redis_client, kube_client, backoff=0.01)
        janitor.hset('rhash', 'key', 'value')
        assert janitor.redis_client.fail_count == redis_client.fail_tolerance

    def test_hget(self):
        redis_client = DummyRedis(fail_tolerance=2)
        kube_client = DummyKubernetes()
        janitor = janitors.RedisJanitor(redis_client, kube_client, backoff=0.01)
        data = janitor.hget('rhash_new', 'status')
        assert data == 'new'
        assert janitor.redis_client.fail_count == redis_client.fail_tolerance

    def test_scan_iter(self):
        prefix = 'predict'
        redis_client = DummyRedis(fail_tolerance=2, prefix=prefix)
        kube_client = DummyKubernetes()
        janitor = janitors.RedisJanitor(redis_client, kube_client, backoff=0.01)
        data = janitor.scan_iter(match=prefix)
        keys = [k for k in data]
        expected = [k for k in redis_client.keys() if k.startswith(prefix)]
        assert janitor.redis_client.fail_count == redis_client.fail_tolerance
        assert keys == expected

    def test_triage(self):
        redis_client = DummyRedis(fail_tolerance=0)
        kube_client = DummyKubernetes()
        janitor = janitors.RedisJanitor(redis_client, kube_client, backoff=0.01)

        janitor.kill_pod = lambda x, y: True

        def pod(key):
            status = 'Failed' if 'failed' in key else 'Running'
            name = 'good_pod' if 'good' in key else 'bad_pod'
            return [Bunch(metadata=Bunch(name=name), status=Bunch(phase=status))]

        # test end point statuses
        assert janitor.triage('goodkey_failed', pod('goodkey_failed')) is True
        assert janitor.triage('goodkey_new', pod('goodkey_new')) is False
        assert janitor.triage('goodkey_done', pod('goodkey_done')) is False

        # test malformed key (no hostname value)
        assert janitor.triage('badhost_weirdstatus',
                              pod('badhost_weirdstatus')) is False

        # test pod not found
        assert janitor.triage('badkey_inprogress', []) is True

        # test in progress with status != Running
        pods = [Bunch(metadata=Bunch(name='good_pod'), status=Bunch(phase='Failed'))]
        assert janitor.triage('goodkey_inprogress', pods) is True

        # test in progress with status = Running with stale update time
        assert janitor.triage('goodkeystale_inprogress',
                              pod('goodkeystale_inprogress')) is True

        # test in progress with status = Running with fresh update time
        assert janitor.triage('goodkey_inprogress',
                              pod('goodkey_inprogress')) is False

        # test no `timestamp_last_status_update`
        assert janitor.triage('goodmalformed_inprogress',
                              pod('goodmalformed_inprogress')) is False

    def test_triage_keys(self):
        redis_client = DummyRedis(fail_tolerance=0)
        kube_client = DummyKubernetes()
        janitor = janitors.RedisJanitor(redis_client, kube_client, backoff=0.01)

        # monkey-patch kubectl commands
        janitor.kill_pod = lambda x: True
        janitor._get_all_pods = lambda: 'good_pod status Running'
        janitor._make_kubectl_call = lambda x: 0

        # run triage_keys
        janitor.triage_keys()
