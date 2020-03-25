# Copyright 2016-2020 The Van Valen Lab at the California Institute of
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

import random
import datetime

import pytz
import kubernetes

import pytest

from redis_janitor import janitors


class Bunch(object):
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class DummyRedis(object):
    # pylint: disable=W0613,R0201
    def __init__(self, prefix='predict', status='new'):
        self.fail_count = 0
        self.prefix = '/'.join(x for x in prefix.split('/') if x)
        self.status = status
        self.keys = [
            '{}:{}:{}'.format(self.prefix, self.status, 'x.tiff'),
            '{}:{}:{}'.format(self.prefix, 'other', 'x.zip'),
            '{}:{}:{}'.format('other', self.status, 'x.TIFF'),
            '{}:{}:{}'.format(self.prefix, self.status, 'x.ZIP'),
            '{}:{}:{}'.format(self.prefix, 'other', 'x.tiff'),
            '{}:{}:{}'.format('other', self.status, 'x.zip'),
        ]

    def _get_dummy_data(self, rhash, identity, updated):
        return {
            'model_name': 'model',
            'model_version': '0',
            'field': '61',
            'cuts': '0',
            'updated_by': identity,
            'status': rhash.split(':')[-1],
            'postprocess_function': '',
            'preprocess_function': '',
            'file_name': rhash.split(':')[-1],
            'input_file_name': rhash.split(':')[-1],
            'output_file_name': rhash.split(':')[-1],
            'updated_at': updated,
        }

    def scan_iter(self, match=None, count=None):
        for k in self.keys:
            if match:
                if k.startswith(match[:-1]):
                    yield k
            else:
                yield k

    def lrem(self, key, count, value, *_, **kwargs):
        return count

    def lpush(self, *_, **__):
        return len(self.keys)

    def hmset(self, rhash, hvals):
        return True

    def hset(self, rhash, status, value):
        return True

    def lrange(self, queue, start, end):
        return self.keys[start:end]

    def hmget(self, rhash, *keys):
        now = datetime.datetime.now(pytz.UTC)
        later = (now - datetime.timedelta(minutes=60))
        identity = 'good_pod' if 'good' in rhash else 'bad_pod'
        identity = 'zip-consumer' if 'whitelist' in rhash else identity
        updated = (later if 'stale' in rhash else now).isoformat(' ')
        updated = None if 'malformed' in rhash else updated
        dummy = self._get_dummy_data(rhash, identity, updated)
        return [dummy.get(k) for k in keys]

    def hgetall(self, rhash):
        now = datetime.datetime.now(pytz.UTC)
        later = (now - datetime.timedelta(minutes=60))
        identity = 'good_pod' if 'good' in rhash else 'bad_pod'
        identity = 'zip-consumer' if 'whitelist' in rhash else identity
        updated = (later if 'stale' in rhash else now).isoformat(' ')
        updated = None if 'malformed' in rhash else updated
        dummy = self._get_dummy_data(rhash, identity, updated)
        return dummy

    def type(self, key):
        return 'hash'


class DummyKubernetes(object):

    def __init__(self, fail=False):
        self.fail = fail

    def delete_namespaced_pod(self, *_, **__):
        if self.fail:
            raise kubernetes.client.rest.ApiException('thrown on purpose')
        return True

    def list_pod_for_all_namespaces(self, *_, **__):
        if self.fail:
            raise kubernetes.client.rest.ApiException('thrown on purpose')
        return Bunch(items=[Bunch(status=Bunch(phase='Running'),
                                  metadata=Bunch(name='pod')),
                            Bunch(status=Bunch(phase='Evicted'),
                                  metadata=Bunch(name='badpod'))])

    def list_namespaced_pod(self, *_, **__):
        if self.fail:
            raise kubernetes.client.rest.ApiException('thrown on purpose')
        return Bunch(items=[Bunch(status=Bunch(phase='Running'),
                                  metadata=Bunch(name='pod')),
                            Bunch(status=Bunch(phase='Evicted'),
                                  metadata=Bunch(name='badpod'))])


class TestJanitor(object):

    def get_client(self, **kwargs):
        if 'backoff' not in kwargs:
            kwargs['backoff'] = 0.01
        if 'queue' not in kwargs:
            kwargs['queue'] = 'q'
        if 'redis_client' not in kwargs:
            kwargs['redis_client'] = DummyRedis()
        janitor = janitors.RedisJanitor(**kwargs)
        janitor.get_core_v1_client = DummyKubernetes
        return janitor

    def test_kill_pod(self):
        janitor = self.get_client()
        assert janitor.kill_pod('pass', 'ns') is True

        janitor.get_core_v1_client = lambda: DummyKubernetes(fail=True)
        assert janitor.kill_pod('fail', 'ns') is False

    def test_list_pod_for_all_namespaces(self):
        janitor = self.get_client()

        expected = DummyKubernetes().list_pod_for_all_namespaces()
        expected = expected.items  # pylint: disable=E1101
        items = janitor.list_pod_for_all_namespaces()
        assert len(items) == len(expected)
        for i in range(len(items)):
            assert items[i].metadata.name == expected[i].metadata.name
            assert items[i].status.phase == expected[i].status.phase

        janitor.get_core_v1_client = lambda: DummyKubernetes(fail=True)

        items = janitor.list_pod_for_all_namespaces()
        assert items == []

    def test_list_namespaced_pods(self):
        janitor = self.get_client()

        expected = DummyKubernetes().list_namespaced_pod()
        expected = expected.items  # pylint: disable=E1101
        items = janitor.list_namespaced_pod()
        for i in range(len(items)):
            assert items[i].metadata.name == expected[i].metadata.name
            assert items[i].status.phase == expected[i].status.phase

        janitor.get_core_v1_client = lambda: DummyKubernetes(fail=True)

        items = janitor.list_namespaced_pod()
        assert items == []

    def test_is_whitelisted(self):
        janitor = self.get_client()

        janitor.whitelisted_pods = ['pod1', 'pod2']
        assert janitor.is_whitelisted('pod1-123-456') is True
        assert janitor.is_whitelisted('pod2-123-456') is True
        assert janitor.is_whitelisted('pod3-123-456') is False

    def test_remove_key_from_queue(self):
        janitor = self.get_client()

        def dummy_lrem(key, count, value):
            total = sum(value == k for k in janitor.redis_client.keys)
            return int(total >= count)

        janitor.redis_client.lrem = dummy_lrem
        valid_key = janitor.redis_client.keys[0]
        invalid_key = 'badkey'
        assert int(janitor.remove_key_from_queue(valid_key)) == 1
        assert int(janitor.remove_key_from_queue(invalid_key)) == 0

    def test_repair_redis_key(self):
        janitor = self.get_client()

        def remove_key(_):
            return True

        # Remove key and put it back in the work queue
        janitor.remove_key_from_queue = remove_key
        janitor.repair_redis_key('testkey')

        def fail_to_remove(_):
            return False

        # Could not remove key, should log it.
        janitor.remove_key_from_queue = fail_to_remove
        janitor.repair_redis_key('testkey')

    def test__update_pods(self):
        janitor = self.get_client()
        janitor._update_pods()
        # pylint: disable=E1101
        expected = DummyKubernetes().list_namespaced_pod().items
        # pylint: enable=E1101
        assert isinstance(janitor.pods_updated_at, datetime.datetime)
        assert len(janitor.pods) == len(expected)
        for e in expected:
            assert e.metadata.name in janitor.pods
            assert janitor.pods[e.metadata.name] == e.status.phase

    def test_udpate_pods(self):
        janitor = self.get_client(pod_refresh_interval=10000)
        janitor.update_pods()
        # pylint: disable=E1101
        expected = DummyKubernetes().list_namespaced_pod().items
        # pylint: enable=E1101
        assert isinstance(janitor.pods_updated_at, datetime.datetime)
        assert len(janitor.pods) == len(expected)
        for e in expected:
            assert e.metadata.name in janitor.pods
            assert janitor.pods[e.metadata.name] == e.status.phase

        # now that we've called it once, lets make sure it doesnt happen again
        janitor.pods = {}  # resetting this for test
        janitor.update_pods()
        assert not janitor.pods  # should still be empty

        # reset refresh interval and try again, should update agaain.
        janitor.pod_refresh_interval = -1
        janitor.update_pods()
        assert len(janitor.pods) == len(expected)

        with pytest.raises(ValueError):
            janitor.pods_updated_at = 1  # wrong type causes error
            janitor.update_pods()

    def test_is_stale_update_time(self):
        new_time = datetime.datetime.now(pytz.UTC)
        old_time = new_time - datetime.timedelta(days=1)

        # first test self.stale_time with default setting (~5 min)
        janitor = self.get_client()
        assert janitor.is_stale_update_time(old_time) is True
        assert janitor.is_stale_update_time(old_time.isoformat()) is True
        assert janitor.is_stale_update_time(new_time) is False
        assert janitor.is_stale_update_time(new_time.isoformat()) is False
        # override default stale_time - not stale
        assert janitor.is_stale_update_time(new_time, int(1e12)) is False
        # overrid default stale_time - disable
        assert janitor.is_stale_update_time(new_time, -1) is False
        # overrid default stale_time - force stale
        janitor = self.get_client(stale_time=-1)
        assert janitor.is_stale_update_time(old_time, 0.001) is True

        # test disabled `stale_time`
        janitor = self.get_client(stale_time=-1)
        assert janitor.is_stale_update_time(old_time) is False
        assert janitor.is_stale_update_time(new_time) is False

        # test invalid update_time
        assert janitor.is_stale_update_time(None) is False
        assert janitor.is_stale_update_time(None, 0) is False

    def test_get_processing_keys(self):
        queue = 'test-queue'
        janitor = self.get_client(queue=queue)
        assert [x for x in janitor.get_processing_keys()] == []

        janitor.redis_client.keys = [
            'processing-{q}:{pod}'.format(q=queue, pod=random.randint(0, 100)),
            'processing-{q}:{pod}'.format(q=queue, pod=random.randint(0, 100)),
            'processing-{q}:{pod}'.format(q=queue, pod=random.randint(0, 100)),
            'other key',
        ]
        expected = janitor.redis_client.keys[0:3]
        assert [x for x in janitor.get_processing_keys()] == expected

    def test_is_valid_pod(self):
        janitor = self.get_client(stale_time=5)
        assert janitor.is_valid_pod('pod') is True  # valid pod name
        assert janitor.is_valid_pod('missing') is False

    def test_clean_key(self):
        janitor = self.get_client(stale_time=5)
        janitor.cleaning_queue = 'processing-q:pod'
        # assert janitor.clean_key('stale:new') is True
        # assert janitor.clean_key('stale:done') is True
        # assert janitor.clean_key('stale:failed') is True
        # assert janitor.clean_key('stale:working') is True
        assert janitor.clean_key('goodkey:new') is False
        assert janitor.clean_key('goodkey:done') is False
        assert janitor.clean_key('goodkey:failed') is False
        assert janitor.clean_key('goodkey:working') is False
        # janitor.cleaning_queue = 'processing-q:missing'
        # assert janitor.clean_key('goodkey:working') is True

        janitor = self.get_client()
        janitor.cleaning_queue = 'processing-q:pod'

        # test status `new`
        assert janitor.clean_key('predict:new') is False

        # test status `done`
        assert janitor.clean_key('predict:done') is False

        janitor = self.get_client(stale_time=60)
        janitor.cleaning_queue = 'processing-q:pod'
        # assert janitor.clean_key('goodkeystale:inprogress') is True

        # test in progress with status = Running with fresh update time
        # assert janitor.clean_key('goodkey:inprogress') is False

        # test no `updated_at`
        assert janitor.clean_key('goodmalformed:inprogress') is False

        # test pod is not found
        # janitor.cleaning_queue = 'processing-q:bad'
        # assert janitor.clean_key('goodkey:inprogress') is True

        # test pod is not found and stale
        janitor.cleaning_queue = 'processing-q:bad'
        assert janitor.clean_key('stalekey:inprogress') is True

    def test_clean(self):
        queues = 'q1,q2'
        num_queues = len(queues.split(','))
        janitor = self.get_client(queue=queues)
        whitelisted = janitor.whitelisted_pods[0]

        keys = []
        for q in queues.split(','):
            keys.extend([
                'processing-{q}:pod'.format(q=q),
                'processing-{q}:{pod}'.format(q=q, pod=whitelisted),
                'processing-{q}:pod'.format(q=q),
            ])
        keys.append('other key')

        janitor.redis_client.keys = keys
        janitor.clean_key = lambda *x: True
        janitor.is_whitelisted = lambda x: int(x) % 2 == 0
        janitor.lrange = []
        janitor.clean()
        assert janitor.total_repairs == (num_queues * 3) ** 2  # valid_keys**2
