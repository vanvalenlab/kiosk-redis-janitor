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
"""Janitor Class"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import timeit
import datetime
import logging

import pytz
import dateutil.parser
import kubernetes.client


class RedisJanitor(object):
    """Clean up keys in the redis queue"""

    def __init__(self,
                 redis_client,
                 queue,
                 queue_delimiter=',',
                 namespace='default',
                 backoff=3,
                 stale_time=600,  # 10 minutes
                 pod_refresh_interval=5,):
        self.redis_client = redis_client
        self.logger = logging.getLogger(str(self.__class__.__name__))
        self.backoff = backoff
        self.queues = str(queue).lower().split(queue_delimiter)
        self.namespace = namespace
        self.stale_time = int(stale_time)
        self.pod_refresh_interval = int(pod_refresh_interval)

        # empty initializers, update them with _update_pods
        self.pods = {}
        self.pods_updated_at = None

        # attributes for managing pod state
        self.whitelisted_pods = ['zip-consumer']
        self.valid_pod_phases = {
            'Running',
            # 'Pending',
            # 'ContainerCreating'
        }

        self.total_repairs = 0
        self.processing_queues = []
        for q in self.queues:
            self.processing_queues.extend([
                'processing-{}'.format(q),
                'processing-{}-zip'.format(q)
            ])
        self.cleaning_queue = ''  # update this in clean()

    def get_core_v1_client(self):
        """Returns Kubernetes API Client for CoreV1Api"""
        kubernetes.config.load_incluster_config()
        return kubernetes.client.CoreV1Api()

    def kill_pod(self, pod_name, namespace):
        # delete the pod
        t = timeit.default_timer()
        try:
            kube_client = self.get_core_v1_client()
            response = kube_client.delete_namespaced_pod(
                pod_name, namespace, grace_period_seconds=0)
        except kubernetes.client.rest.ApiException as err:
            self.logger.error('`delete_namespaced_pod` encountered %s: %s. '
                              'Failed to kill pod `%s.%s`',
                              type(err).__name__, err, namespace, pod_name)
            return False
        self.logger.debug('Killed pod `%s` in namespace `%s` in %s seconds.',
                          pod_name, namespace, timeit.default_timer() - t)
        return response

    def list_pod_for_all_namespaces(self):
        start = timeit.default_timer()
        try:
            kube_client = self.get_core_v1_client()
            response = kube_client.list_pod_for_all_namespaces()
        except kubernetes.client.rest.ApiException as err:
            self.logger.error('`list_pod_for_all_namespaces` encountered '
                              '%s: %s.', type(err).__name__, err)
            return []
        self.logger.debug('Found %s pods in %s seconds.',
                          len(response.items), timeit.default_timer() - start)
        return response.items

    def list_namespaced_pod(self):
        start = timeit.default_timer()
        try:
            kube_client = self.get_core_v1_client()
            response = kube_client.list_namespaced_pod(self.namespace)
        except kubernetes.client.rest.ApiException as err:
            self.logger.error('`list_namespaced_pod %s` encountered %s: %s',
                              self.namespace, type(err).__name__, err)
            return []
        self.logger.debug('Found %s pods in namespace `%s` in %s seconds.',
                          len(response.items), self.namespace,
                          timeit.default_timer() - start)
        return response.items

    def get_processing_keys(self, count=100):
        for q in self.processing_queues:
            match = '{}:*'.format(q)
            keys = self.redis_client.scan_iter(match=match, count=count)
            for key in keys:
                yield key

    def is_whitelisted(self, pod_name):
        """Ignore missing pods that are whitelisted"""
        pod_name = str(pod_name)
        return any(pod_name.startswith(x) for x in self.whitelisted_pods)

    def remove_key_from_queue(self, redis_key):
        start = timeit.default_timer()
        res = self.redis_client.lrem(self.cleaning_queue, 1, redis_key)
        if res:
            self.logger.debug('Removed key `%s` from `%s` in %s seconds.',
                              redis_key, self.cleaning_queue,
                              timeit.default_timer() - start)
        else:
            self.logger.warning('Failed to remove key `%s` from queue `%s`.',
                                redis_key, self.cleaning_queue)
        return res

    def repair_redis_key(self, redis_key):
        is_removed = self.remove_key_from_queue(redis_key)
        if is_removed:
            start = timeit.default_timer()
            source_queue = self.cleaning_queue.split(':')[0]
            source_queue = source_queue.split('processing-')[-1]
            self.redis_client.lpush(source_queue, redis_key)
            self.logger.debug('Pushed key `%s` to `%s` in %s seconds.',
                              redis_key, source_queue,
                              timeit.default_timer() - start)
        else:
            self.logger.warning('Tried to repair key %s but it was no longer '
                                'in %s', redis_key, self.cleaning_queue)
        return is_removed

    def _update_pods(self):
        """Refresh pod data and update timestamp"""
        namespaced_pods = self.list_pod_for_all_namespaces()
        self.pods = {pod.metadata.name: pod.status.phase
                     for pod in namespaced_pods}
        self.pods_updated_at = datetime.datetime.now(pytz.UTC)

    def update_pods(self):
        """Calls `_update_pods` if longer than `pod_refresh_interval`"""
        if self.pods_updated_at is None:
            self._update_pods()
        elif not isinstance(self.pods_updated_at, datetime.datetime):
            raise ValueError('`update_pods` expected `pods_updated_at` to be'
                             ' a `datetime.datetime` instance got %s.' %
                             type(self.pods_updated_at).__name__)
        else:
            diff = datetime.datetime.now(pytz.UTC) - self.pods_updated_at
            if diff.total_seconds() > self.pod_refresh_interval:
                self._update_pods()

    def is_valid_pod(self, pod_name):
        self.update_pods()  # only updates if stale
        is_valid = self.pods.get(pod_name) in self.valid_pod_phases
        return is_valid

    def _timestamp_to_age(self, ts):
        if ts is None:
            return 0  # key is new

        if not isinstance(ts, datetime.datetime):
            # TODO: `dateutil` deprecated by python 3.7 `fromisoformat`
            # ts = datetime.datetime.fromisoformat(ts)
            ts = dateutil.parser.parse(str(ts))
        current_time = datetime.datetime.now(pytz.UTC)
        diff = current_time - ts
        return diff.total_seconds()

    def is_stale_update_time(self, updated_time, stale_time=None):
        stale_time = stale_time if stale_time else self.stale_time
        if not updated_time:
            return False
        if stale_time <= 0:
            return False
        last_updated = self._timestamp_to_age(updated_time)
        return last_updated >= stale_time

    def should_clean_key(self, key, updated_ts):
        """Return a boolean if the key should be cleaned"""
        pod_name = self.cleaning_queue.split(':')[-1]

        updated_seconds = self._timestamp_to_age(updated_ts)

        if updated_seconds <= self.pod_refresh_interval * 3:
            return False  # this is too fresh for our pod data

        if self.is_valid_pod(pod_name):  # pod exists in a valid state
            # if not self.is_stale_update_time(updated_ts):
            #     return False  # pod exists and key is updated recently
            #
            # # pod exists but key is stale
            # self.logger.warning('Key `%s` in queue `%s` was last updated at '
            #                     '`%s` (%s seconds ago) and pod `%s` is still '
            #                     'alive with status %s but is_stale turned off.',
            #                     key, self.cleaning_queue, updated_ts,
            #                     updated_seconds, pod_name,
            #                     self.pods[pod_name])
            # # self.kill_pod(pod_name, self.namespace)
            return False

        # pod is not valid
        if pod_name not in self.pods:  # pod does not exist
            self.logger.info('Key `%s` in queue `%s` was last updated by pod '
                             '`%s` %s seconds ago, but that pod does not '
                             'exist.', key, self.cleaning_queue, pod_name,
                             updated_seconds)
        else:  # pod exists but has a bad status
            self.logger.info('Key `%s` in queue `%s` was last updated by '
                             'pod `%s` %s seconds ago, but that pod has status'
                             ' %s.', key, self.cleaning_queue, pod_name,
                             updated_seconds, self.pods[pod_name])
        return True

    def clean_key(self, key):
        required_keys = [
            'status',
            'updated_at',
            'updated_by',
        ]
        res = self.redis_client.hmget(key, *required_keys)
        hvals = dict(zip(required_keys, res))

        if not any(res):  # No values found in the key
            self.logger.warning('Removing invalid key `%s`.', key)
            return bool(self.remove_key_from_queue(key))

        should_clean = self.should_clean_key(key, hvals.get('updated_at'))

        if should_clean:
            # key in the processing queue is either stranded or stale
            # if the key is finished already, just remove it from the queue
            if hvals.get('status') in {'done', 'failed'}:
                return bool(self.remove_key_from_queue(key))

            # if the job is not finished, repair the key
            return bool(self.repair_redis_key(key))

        return should_clean

    def clean(self):
        cleaned = 0

        for q in self.get_processing_keys(count=100):
            self.cleaning_queue = q
            for i, key in enumerate(self.redis_client.lrange(q, 0, -1)):
                if i >= 1:
                    self.logger.warning('Queue `%s` has an item with index %s.'
                                        ' This is strange.', q, i)
                is_key_cleaned = self.clean_key(key)
                if is_key_cleaned:
                    self.logger.info('Repaired key `%s` from queue `%s`',
                                     key, q)
                cleaned = cleaned + int(is_key_cleaned)

        if cleaned:  # loop is finished, summary log
            self.total_repairs += cleaned
            self.logger.info('Repaired %s key%s (%s total).', cleaned,
                             's' if cleaned else '', self.total_repairs)

        # reset state to like new
        self.cleaning_queue = ''
        self.pods = {}
        self.pods_updated_at = None
