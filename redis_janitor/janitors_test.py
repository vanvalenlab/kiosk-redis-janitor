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

import datetime

import pytz
import kubernetes

from redis_janitor import janitors


class Bunch(object):
    def __init__(self, **kwds):
        self.__dict__.update(kwds)


class DummyRedis(object):
    def __init__(self, prefix='predict', status='new',
                 fail_tolerance=0, hard_fail=False):
        self.hard_fail = hard_fail
        self.fail_count = 0
        self.fail_tolerance = fail_tolerance
        self.prefix = '/'.join(x for x in prefix.split('/') if x)
        self.status = status
        self.keys = [
            '{}_{}_{}'.format(self.prefix, self.status, 'x.tiff'),
            '{}_{}_{}'.format(self.prefix, 'other', 'x.zip'),
            '{}_{}_{}'.format('other', self.status, 'x.TIFF'),
            '{}_{}_{}'.format(self.prefix, self.status, 'x.ZIP'),
            '{}_{}_{}'.format(self.prefix, 'other', 'x.tiff'),
            '{}_{}_{}'.format('other', self.status, 'x.zip'),
        ]

    def scan_iter(self, match=None, count=None):
        if match:
            return (k for k in self.keys if k.startswith(match[:-1]))
        return (k for k in self.keys)

    def lrem(self, *_, **__):
        return True

    def lpush(self, *_, **__):
        return True

    def hmset(self, rhash, hvals):  # pylint: disable=W0613
        return hvals

    def hset(self, rhash, status, value):  # pylint: disable=W0613
        return {status: value}

    def hgetall(self, rhash):  # pylint: disable=W0613
        now = datetime.datetime.now(pytz.UTC)
        later = (now - datetime.timedelta(minutes=3))
        identity = 'good_pod' if 'good' in rhash else 'bad_pod'
        identity = None if 'badhost' in rhash else identity
        updated = (later if 'stale' in rhash else now).isoformat(' ')
        updated = None if 'malformed' in rhash else updated
        return {
            'model_name': 'model',
            'model_version': '0',
            'field': '61',
            'cuts': '0',
            'identity_started': identity,
            'status': rhash.split(':')[-1],
            'postprocess_function': '',
            'preprocess_function': '',
            'file_name': rhash.split(':')[-1],
            'input_file_name': rhash.split(':')[-1],
            'output_file_name': rhash.split(':')[-1],
            'updated_at': updated,
        }

    def type(self, key):  # pylint: disable=W0613
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
                                  metadata=Bunch(name='pod'))])


class TestJanitor(object):

    def test_kill_pod(self):
        redis_client = DummyRedis(fail_tolerance=2)
        janitor = janitors.RedisJanitor(redis_client, 'q', backoff=0.01)
        janitor.get_core_v1_client = DummyKubernetes
        assert janitor.kill_pod('pass', 'ns') is True

        janitor.get_core_v1_client = lambda: DummyKubernetes(fail=True)
        assert janitor.kill_pod('fail', 'ns') is False

    def test_list_pod_for_all_namespaces(self):
        redis_client = DummyRedis(fail_tolerance=2)
        janitor = janitors.RedisJanitor(redis_client, 'q', backoff=0.01)
        janitor.get_core_v1_client = DummyKubernetes

        items = janitor.list_pod_for_all_namespaces()
        assert len(items) == 1 and items[0].metadata.name == 'pod'

        janitor.get_core_v1_client = lambda: DummyKubernetes(fail=True)

        items = janitor.list_pod_for_all_namespaces()
        assert items == []

    def test_triage(self):
        redis_client = DummyRedis(fail_tolerance=0)
        janitor = janitors.RedisJanitor(redis_client, 'q', 0)

        def pod(key):
            status = 'Failed' if 'failed' in key else 'Running'
            name = 'good_pod' if 'good' in key else 'bad_pod'
            return {name: Bunch(metadata=Bunch(name=name),
                                status=Bunch(phase=status))}

        # test end point statuses
        assert janitor.triage('goodkey:new', pod('goodkey:new')) is False
        assert janitor.triage('goodkey:done', pod('goodkey:done')) is False

        # test failed status
        janitor = janitors.RedisJanitor(redis_client, 'q', 0,
                                        restart_failures=True)
        assert janitor.triage('goodkey:failed', pod('goodkey:failed')) is True

        janitor = janitors.RedisJanitor(redis_client, 'q', 0)
        assert janitor.triage('goodkey:failed', pod('goodkey:failed')) is False

        # test malformed key (no hostname value)
        assert janitor.triage('badhost:weirdstatus',
                              pod('badhost:weirdstatus')) is False

        # test pod not found
        janitor.kill_pod = lambda x, y: True
        assert janitor.triage('badkey_inprogress', {}) is True

        # test in progress with status != Running
        pods = {'good_pod': Bunch(metadata=Bunch(name='good_pod'),
                                  status=Bunch(phase='Failed'))}
        assert janitor.triage('goodkey:inprogress', pods) is True

        # test in progress with status = Running with stale update time
        assert janitor.triage('goodkeystale:inprogress',
                              pod('goodkeystale:inprogress')) is False

        janitor = janitors.RedisJanitor(redis_client, 'q', 0, stale_time=60)
        assert janitor.triage('goodkeystale:inprogress',
                              pod('goodkeystale:inprogress')) is True

        # test in progress with status = Running with fresh update time
        assert janitor.triage('goodkey:inprogress',
                              pod('goodkey:inprogress')) is False

        # test no `updated_at`
        assert janitor.triage('goodmalformed:inprogress',
                              pod('goodmalformed:inprogress')) is False

    def test_triage_keys(self):
        redis_client = DummyRedis(fail_tolerance=0)
        janitor = janitors.RedisJanitor(redis_client, 'q', backoff=0.01)
        janitor.get_core_v1_client = DummyKubernetes

        # monkey-patch kubectl commands
        janitor.kill_pod = lambda x: True
        janitor._get_all_pods = lambda: 'good_pod status Running'
        janitor._make_kubectl_call = lambda x: 0

        # run triage_keys
        janitor.triage_keys()
