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
                 namespace='default',
                 backoff=3,
                 stale_time=600,  # 10 minutes
                 restart_failures=False,
                 failure_stale_seconds=60,
                 pod_refresh_interval=10,):
        self.redis_client = redis_client
        self.logger = logging.getLogger(str(self.__class__.__name__))
        self.backoff = backoff
        self.queue = str(queue).lower()
        self.namespace = namespace
        self.stale_time = int(stale_time)
        self.restart_failures = restart_failures
        self.failure_stale_seconds = failure_stale_seconds
        self.pod_refresh_interval = int(pod_refresh_interval)

        # empty initializers, update them with _update_pods
        self.pods = {}
        self.pods_updated_at = None

        # attributes for managing pod state
        self.whitelisted_pods = ['zip-consumer']
        self.valid_pod_phases = {'Running', 'Pending'}

        self.total_repairs = 0
        self.processing_queue = 'processing-{}'.format(self.queue)

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
            self.logger.error('`delete_namespaced_pod` encountered %s: %s.',
                              type(err).__name__, err)
            return False
        self.logger.debug('Killed pod `%s` in namespace `%s` in %s seconds.',
                          pod_name, namespace, timeit.default_timer() - t)
        return response

    def list_pod_for_all_namespaces(self):
        t = timeit.default_timer()
        try:
            kube_client = self.get_core_v1_client()
            response = kube_client.list_pod_for_all_namespaces()
        except kubernetes.client.rest.ApiException as err:
            self.logger.error('`list_pod_for_all_namespaces` encountered '
                              '%s: %s.', type(err).__name__, err)
            return []
        self.logger.debug('Found %s pods in %s seconds.',
                          len(response.items), timeit.default_timer() - t)
        return response.items

    def reset_redis_key(self, redis_key):
        # remove the key from the processing queue, if present
        self.redis_client.lrem(self.processing_queue, 1, redis_key)
        # set the status back to `new`
        self.redis_client.hset(redis_key, 'status', 'new')
        # push the key back into the work queue
        self.redis_client.lpush(self.queue, redis_key)

    def triage(self, key, all_pods):
        hvals = self.redis_client.hgetall(key)
        key_status = hvals.get('status')

        if key_status in {'new', 'done'}:
            # either not started or successuflly completed. no action taken.
            return False

        # TODO: some failures should be restarted, other not
        if key_status == 'failed':
            reset_text = ' Resetting it now.' if self.restart_failures else ''
            self.logger.info('Key %s failed due to "%s".%s',
                             key, hvals.get('reason', 'REASON NOT FOUND'),
                             reset_text)
            if self.restart_failures:
                self.reset_redis_key(key)
            return self.restart_failures

        # status is not a beginning or ending status,
        # but is the key actively being processed?

        # is the pod processing this key alive?
        host = hvals.get('identity_started')
        if not host:
            # if the status is not `new`, we would expect there to be that
            # the host that changed the status would be `identity_started`
            self.logger.warning('Key `%s` has status `%s` but no value for'
                                ' `identity_started`.',
                                key, self.redis_client.hgetall(key))
            return False

        pod = all_pods.get(host)

        # does the pod that started the key still exist?
        if not pod:
            self.logger.info('Key `%s` was started by pod `%s`, but this '
                             'pod cannot be found. Resetting key.',
                             key, host)
            self.reset_redis_key(key)
            return True

        # is the pod still running?
        if pod.status.phase != 'Running':
            # we need to make sure it gets killed.
            # and then reset the status of the job
            self.logger.info('Pod %s is in status `%s`.  Killing it '
                             'and then resetting record `%s`.',
                             host, pod.status.phase, key)
            self.kill_pod(host, 'deepcell')  # TODO: hardcoded namespace
            self.reset_redis_key(key)
            return True

        # has the key's status been updated in the last N seconds?
        try:
            updated_time = hvals.get('updated_at')
            # TODO: `dateutil` deprecated by python 3.7 `fromisoformat`
            # updated_time = datetime.datetime.fromisoformat(updated_time)
            updated_time = dateutil.parser.parse(updated_time)
            current_time = datetime.datetime.now(pytz.UTC)
            update_diff = current_time - updated_time
        except (TypeError, ValueError):
            self.logger.info('Key `%s` has status `%s` but has no '
                             '`updated_at` field.', key, key_status)
            return False

        if update_diff.total_seconds() >= self.stale_time > 0:
            # This entry has not been updated in at least `timeout_seconds`
            # Assume it has died, and reset the status
            self.logger.info('Key `%s` has not been updated in %s seconds, '
                             'which is longer than %s seconds. Resetting it '
                             'now.', key, update_diff, self.stale_time)
            self.reset_redis_key(key)
            return True

        return False

    def triage_keys(self):
        repairs = 0
        pods = self.list_pod_for_all_namespaces()
        pod_dict = {p.metadata.name: p for p in pods}
        self.logger.info('Found %s pods.', len(pods))

        for key in self.redis_client.scan_iter(count=1000):
            if self.redis_client.type(key) == 'hash':
                key_repaired = self.triage(key, pod_dict)
                num_repaired = int(key_repaired)
                repairs += num_repaired
                self._repairs += num_repaired
                if num_repaired:
                    self.logger.info('Repaired key: `%s`.', key)

        self.logger.info('Repaired %s keys (%s total).',
                         repairs, self._repairs)
