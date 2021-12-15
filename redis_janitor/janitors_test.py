# Copyright 2016-2021 The Van Valen Lab at the California Institute of
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

import fakeredis
import pytest

from redis_janitor import janitors


@pytest.fixture
def redis_client():
    yield fakeredis.FakeStrictRedis(decode_responses='utf8')


@pytest.fixture
def janitor(mocker, redis_client):  # pylint: disable=W0621
    mocker.patch('kubernetes.config.load_incluster_config')
    mocker.patch('kubernetes.client.CoreV1Api', DummyKubernetes)
    queues = 'p,q'
    yield janitors.RedisJanitor(redis_client, queues, backoff=0)


def kube_error(*_, **__):
    raise kubernetes.client.rest.ApiException('thrown on purpose')


class Bunch(object):
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class DummyKubernetes(object):
    # pylint: disable=R0201

    def delete_namespaced_pod(self, *_, **__):
        return True

    def list_pod_for_all_namespaces(self, *_, **__):
        return Bunch(items=[Bunch(status=Bunch(phase='Running'),
                                  metadata=Bunch(name='pod')),
                            Bunch(status=Bunch(phase='Evicted'),
                                  metadata=Bunch(name='badpod'))])

    def list_namespaced_pod(self, *_, **__):
        return Bunch(items=[Bunch(status=Bunch(phase='Running'),
                                  metadata=Bunch(name='pod')),
                            Bunch(status=Bunch(phase='Evicted'),
                                  metadata=Bunch(name='badpod'))])


class TestJanitor(object):
    # pylint: disable=W0621,R0201

    def test_kill_pod(self, mocker, janitor):
        assert janitor.kill_pod('pass', 'ns') is True

        mocker.patch('kubernetes.client.CoreV1Api.delete_namespaced_pod',
                     kube_error)
        assert janitor.kill_pod('fail', 'ns') is False

    def test_list_pod_for_all_namespaces(self, mocker, janitor):
        expected = DummyKubernetes().list_pod_for_all_namespaces()
        expected = expected.items  # pylint: disable=E1101
        items = janitor.list_pod_for_all_namespaces()
        assert len(items) == len(expected)
        for i in range(len(items)):
            assert items[i].metadata.name == expected[i].metadata.name
            assert items[i].status.phase == expected[i].status.phase

        mocker.patch('kubernetes.client.CoreV1Api.list_pod_for_all_namespaces',
                     kube_error)

        items = janitor.list_pod_for_all_namespaces()
        assert items == []

    def test_list_namespaced_pods(self, mocker, janitor):
        expected = DummyKubernetes().list_namespaced_pod()
        expected = expected.items  # pylint: disable=E1101
        items = janitor.list_namespaced_pod()
        for i in range(len(items)):
            assert items[i].metadata.name == expected[i].metadata.name
            assert items[i].status.phase == expected[i].status.phase

        mocker.patch('kubernetes.client.CoreV1Api.list_namespaced_pod', kube_error)
        items = janitor.list_namespaced_pod()
        assert items == []

    def test_is_whitelisted(self, janitor):
        janitor.whitelisted_pods = ['pod1', 'pod2']
        assert janitor.is_whitelisted('pod1-123-456') is True
        assert janitor.is_whitelisted('pod2-123-456') is True
        assert janitor.is_whitelisted('pod3-123-456') is False

    def test_remove_key_from_queue(self, janitor):
        # get a queue and put a key in it.
        q = '{}:pod'.format(random.choice(janitor.processing_queues))
        janitor.cleaning_queue = q
        valid_key = random.randint(1, 10)
        invalid_key = random.randint(11, 20)
        janitor.redis_client.lpush(q, valid_key)
        # test remove the key in the queue is successful
        assert int(janitor.remove_key_from_queue(valid_key)) == 1
        # test removing key not in the queue fails
        assert int(janitor.remove_key_from_queue(invalid_key)) == 0

    def test_repair_redis_key(self, janitor):
        # Remove key and put it back in the work queue
        q = '{}:pod'.format(random.choice(janitor.processing_queues))
        janitor.cleaning_queue = q
        key = 'test_key'
        janitor.redis_client.lpush(q, key)
        assert janitor.repair_redis_key(key)
        # Could not remove key, should log it.
        assert not janitor.repair_redis_key('other key')

    def test__update_pods(self, janitor):
        janitor._update_pods()
        # pylint: disable=E1101
        expected = DummyKubernetes().list_namespaced_pod().items
        # pylint: enable=E1101
        assert isinstance(janitor.pods_updated_at, datetime.datetime)
        assert len(janitor.pods) == len(expected)
        for e in expected:
            assert e.metadata.name in janitor.pods
            assert janitor.pods[e.metadata.name] == e.status.phase

    def test_update_pods(self, janitor):
        janitor.pod_refresh_interval = 100000
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

    def test_is_stale_update_time(self, janitor):
        new_time = datetime.datetime.now(pytz.UTC)
        old_time = new_time - datetime.timedelta(days=1)

        # first test self.stale_time with default setting (~5 min)
        assert janitor.is_stale_update_time(old_time) is True
        assert janitor.is_stale_update_time(old_time.isoformat()) is True
        assert janitor.is_stale_update_time(new_time) is False
        assert janitor.is_stale_update_time(new_time.isoformat()) is False
        # override default stale_time - not stale
        assert janitor.is_stale_update_time(new_time, int(1e12)) is False
        # overrid default stale_time - disable
        assert janitor.is_stale_update_time(new_time, -1) is False
        # overrid default stale_time - force stale
        janitor.stale_time = -1
        assert janitor.is_stale_update_time(old_time, 0.001) is True

        # test disabled `stale_time`
        assert janitor.is_stale_update_time(old_time) is False
        assert janitor.is_stale_update_time(new_time) is False

        # test invalid update_time
        assert janitor.is_stale_update_time(None) is False
        assert janitor.is_stale_update_time(None, 0) is False
        assert janitor._timestamp_to_age(None) == 0

    def test_get_processing_keys(self, janitor):
        keys = []
        for i, q in enumerate(janitor.processing_queues):
            key = '{}:{}'.format(q, i)
            janitor.redis_client.lpush(q, key)
            janitor.redis_client.hmset(key, {'test': 1})
            keys.append(key)

        for q in janitor.processing_queues:
            assert isinstance(q, str)
        # add a weird key that isn't parsed properly
        keys.append('other key')

        expected = set(keys[:-1])  # not the last one
        assert set(list(janitor.get_processing_keys())) == expected

        # no processing queues means no processing keys
        janitor.processing_queues = []
        assert list(janitor.get_processing_keys()) == []

    def test_is_valid_pod(self, janitor):
        assert janitor.is_valid_pod('pod') is True  # valid pod name
        assert janitor.is_valid_pod('missing') is False

    def test_should_clean_key(self, janitor):
        processing_queue = random.choice(janitor.processing_queues)
        janitor.pod_refresh_interval = 10

        pods = ['pod', 'badpod', 'missing']

        new_time = datetime.datetime.now(pytz.UTC)
        old_time = new_time - datetime.timedelta(days=1)
        # set timestamps to ISO format
        new_time = new_time.isoformat()
        old_time = old_time.isoformat()

        # test new timestamps are not cleaned for all pod statuses.
        for pod in pods:
            # update cleaning queue to properly get pod_name
            janitor.cleaning_queue = '{}:{}'.format(processing_queue, pod)
            # test no updated_at time will not be cleaned
            assert not janitor.should_clean_key(pod, None)
            # test fresh update time is not cleaned
            assert not janitor.should_clean_key(pod, new_time)

        # test old update time and valid pod is not cleaned
        expected = [False, True, True]
        for p, e in zip(pods, expected):
            janitor.cleaning_queue = '{}:{}'.format(processing_queue, p)
            assert janitor.should_clean_key('key', old_time) is e

    def test_clean_key(self, mocker, janitor):
        q = '{}:pod'.format(random.choice(janitor.processing_queues))
        janitor.cleaning_queue = q

        # add value to queue but value is not a redis hash.
        key = 'test_key'
        janitor.redis_client.lpush(q, key)
        spy = mocker.spy(janitor, 'remove_key_from_queue')
        assert janitor.clean_key(key) is True
        spy.assert_called_once_with(key)

        # add required data to redis hash
        new_time = datetime.datetime.now(pytz.UTC)
        mocker.patch('redis_janitor.janitors.RedisJanitor.should_clean_key',
                     lambda *x: False)
        data = {'status': 'new', 'updated_at': new_time.isoformat()}
        janitor.redis_client.lpush(q, key)
        janitor.redis_client.hmset(key, data)
        assert janitor.clean_key(key) is False

        # test finished status is removed
        mocker.patch('redis_janitor.janitors.RedisJanitor.should_clean_key',
                     lambda *x: True)
        data = {'status': 'done', 'updated_at': new_time.isoformat()}
        janitor.redis_client.lpush(q, key)
        janitor.redis_client.hmset(key, data)
        assert janitor.clean_key(key) is True
        spy.assert_called_with(key)

        # test unfinished status is repaired
        data = {'status': 'not done', 'updated_at': new_time.isoformat()}
        janitor.redis_client.lpush(q, key)
        janitor.redis_client.hmset(key, data)
        spy = mocker.spy(janitor, 'repair_redis_key')
        assert janitor.clean_key(key) is True
        spy.assert_called_once_with(key)

    def test_clean(self, janitor):
        whitelisted = janitor.whitelisted_pods[0]
        new_time = datetime.datetime.now(pytz.UTC)
        old_time = new_time - datetime.timedelta(days=1)
        keys = []
        data = {
            'status': 'new',
            'updated_at': old_time.isoformat(),
            'updated_by': 'test'
        }
        for i, processing_queue in enumerate(janitor.processing_queues):
            if i == 0:
                pod = whitelisted
            elif i == len(janitor.processing_queues) - 1:
                pod = 'missing'
            else:
                pod = 'pod'

            queue = '{}:{}'.format(processing_queue, pod)
            key = 'key{}'.format(i)
            janitor.redis_client.lpush(queue, key)
            janitor.redis_client.hmset(key, data)
            keys.append(key)
            if i == 0:
                # push duplicate key to queue
                janitor.redis_client.lpush(queue, key)

        janitor.clean()
        assert janitor.total_repairs == len(keys) - 2 + 1
