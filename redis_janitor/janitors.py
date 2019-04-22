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

import kubernetes.client


class RedisJanitor(object):

    def __init__(self, redis_client, backoff=3):
        self.redis_client = redis_client
        self._repairs = 0
        self.logger = logging.getLogger(str(self.__class__.__name__))
        self.backoff = backoff

    def get_core_v1_client(self):
        """Returns Kubernetes API Client for CoreV1Api"""
        kubernetes.config.load_incluster_config()
        kube_client = kubernetes.client.CoreV1Api()
        return kube_client

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

    def triage(self, key, all_pods):
        key_status = self.redis_client.hget(key, 'status')

        if key_status not in {'new', 'done', 'failed'}:
            # is the pod processing this key alive?
            host = self.redis_client.hget(key, 'identity_started')

            if not host:
                self.logger.warning('Entry `%s` is malformed. %s',
                                    key, self.redis_client.hgetall(key))
                return False

            try:
                pod = [p for p in all_pods if p.metadata.name == host][0]
            except IndexError:
                self.logger.info('Pod %s not found. Resetting record `%s`.',
                                 host, key)
                self.redis_client.hset(key, 'status', 'new')
                return True

            # the pod's still around, but is something wrong with it?
            if pod.status.phase != 'Running':
                # we need to make sure it gets killed
                # and then reset the status of the job
                self.logger.info('Pod %s is in status `%s`.  Killing it '
                                 'and then resetting record %s.',
                                 host, pod.status.phase, key)
                self.kill_pod(host, 'deepcell')  # TODO: hardcoded namespace
                self.redis_client.hset(key, 'status', 'new')
                return True

            # has the key's status been updated in the last N seconds?
            timeout_seconds = 300
            try:
                fmt = '%b %d, %Y %H:%M:%S.%f'
                current_time = datetime.datetime.now(datetime.timezone.utc)
                current_time = current_time.strftime(fmt)
                updated_time = self.redis_client.hget(key, 'updated_at')
                parse = lambda x: datetime.datetime.strptime(x, fmt)
                update_diff = parse(current_time) - parse(updated_time)
            except (TypeError, ValueError) as err:
                self.logger.info('Key %s with information %s has no '
                                 'appropriate `updated_at` '
                                 'field. %s: %s', key,
                                 self.redis_client.hgetall(key),
                                 type(err).__name__, err)
                return False

            if update_diff.total_seconds() >= timeout_seconds:
                # This entry has not been updated in at least `timeout_seconds`
                # Assume it has died, and reset the status
                self.logger.info('Key `%s` has not been updated in %s seconds.'
                                 ' Resetting its status now.',
                                 key, update_diff)
                self.redis_client.hset(key, 'status', 'new')
                return True

        elif key_status == 'failed':  # TODO: should we restart all failures?
            # key failed, so reset it
            failure_reason = self.redis_client.hget(key, 'reason')
            self.logger.info('Key %s failed due to "%s". Resetting its '
                             'status now.', key, failure_reason)
            self.redis_client.hset(key, 'status', 'new')
            return True

        return False

    def triage_keys(self):
        # or, 1,000 reasons to restart a key
        repairs = 0

        # get list of all pods
        pods = self.list_pod_for_all_namespaces()
        self.logger.info('Found %s pods.', len(pods))

        for key in self.redis_client.scan_iter(count=1000):
            if self.redis_client.type(key) == 'hash':
                key_repaired = self.triage(key, pods)
                num_repaired = int(key_repaired)
                repairs += num_repaired
                self._repairs += num_repaired
                if num_repaired:
                    self.logger.info('Repaired key: `%s`.', key)

        self.logger.info('Repaired %s keys (%s total).',
                         repairs, self._repairs)
