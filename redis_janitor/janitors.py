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

import re
import time
import timeit
import logging
import subprocess

import redis


class RedisJanitor(object):  # pylint: disable=useless-object-inheritance

    def __init__(self, redis_client, backoff=3):
        self.redis_client = redis_client
        self._repairs = 0
        self.logger = logging.getLogger(str(self.__class__.__name__))
        self.backoff = backoff

    def _make_kubectl_call(self, args):
        argstring = ' '.join(args)
        try:
            t = timeit.default_timer()
            subprocess.run(args)
            self.logger.debug('Executed subprocess: `%s` in %s seconds.',
                              argstring, timeit.default_timer() - t)
        except subprocess.CalledProcessError as err:
            # Who knows what's going on.
            # Let it go and see if the parent loop brings us back around.
            self.logger.warning('Encountered %s while executing `%s`. '
                                'Possibly retrying soon...',
                                type(err).__name__, err)
            time.sleep(self.backoff)

    def _get_pod_string(self, args):
        argstring = ' '.join(args)
        while True:
            try:
                t = timeit.default_timer()
                pods_info = subprocess.check_output(args)
                pods = pods_info.decode('utf8')
                self.logger.debug('Executed subprocess: `%s` in %s seconds.',
                                  argstring, timeit.default_timer() - t)
                break
            except subprocess.CalledProcessError as err:
                # For some reason, we can't execute this command right now.
                # Keep trying until we can.
                self.logger.warning('Encountered %s: %s while executing `%s`. '
                                    'Retrying in %s seconds...', argstring,
                                    type(err).__name__, err, self.backoff)
                time.sleep(self.backoff)
        return '\n'.join(x for x in pods.splitlines() if x)

    def kill_pod(self, host):
        # delete the pod
        kill_args = ['kubectl', 'delete', 'pods', host]
        while True:  # repeat until the pod is gone
            self._make_kubectl_call(kill_args)
            try:
                pods_str = self._get_pod_string(kill_args)
                _ = re.search(r'%s +\S+ +(\S+)' % host, pods_str).group(1)
            except AttributeError:
                self.logger.info('Pod %s successfully deleted', host)
                break  # pod no longer exists
            except subprocess.CalledProcessError as err:
                # For some reason, we can't execute this command right now.
                # Keep trying until we can.
                self.logger.warning('Encountered %s: %s while executing with '
                                    'parameters: %s.  etrying in %s seconds...',
                                    kill_args, type(err).__name__, err,
                                    self.backoff)
                time.sleep(self.backoff)

    def hset(self, rhash, key, value):
        while True:
            try:
                response = self.redis_client.hset(rhash, key, value)
                break
            except (ConnectionError, redis.exceptions.ConnectionError) as err:
                self.logger.warning('Encountered %s: %s when calling HSET. '
                                    'Retrying in %s seconds.',
                                    type(err).__name__, err, self.backoff)
                time.sleep(self.backoff)
            except Exception as err:
                self.logger.error('Unexpected %s: %s when calling HSET.',
                                  type(err).__name__, err)
                raise err
        return response

    def scan_iter(self, match=None):
        while True:
            try:
                response = self.redis_client.scan_iter(match=match)
                break
            except (ConnectionError, redis.exceptions.ConnectionError) as err:
                self.logger.warning('Encountered %s: %s when calling SCAN. '
                                    'Retrying in %s seconds.',
                                    type(err).__name__, err, self.backoff)
                time.sleep(self.backoff)
            except Exception as err:
                self.logger.error('Unexpected %s: %s when calling SCAN.',
                                  type(err).__name__, err)
                raise err
        return response

    def _redis_type(self, redis_key):
        while True:
            try:
                response = self.redis_client.type(redis_key)
                break
            except (ConnectionError, redis.exceptions.ConnectionError) as err:
                self.logger.warning('Encountered %s: %s when calling TYPE. '
                                    'Retrying in %s seconds.',
                                    type(err).__name__, err, self.backoff)
                time.sleep(self.backoff)
            except Exception as err:
                self.logger.error('Unexpected %s: %s when calling TYPE.',
                                  type(err).__name__, err)
                raise err
        return response

    def hget(self, rhash, key):
        while True:
            try:
                response = self.redis_client.hget(rhash, key)
                break
            except (ConnectionError, redis.exceptions.ConnectionError) as err:
                self.logger.warning('Encountered %s: %s when calling HGET. '
                                    'Retrying in %s seconds.',
                                    type(err).__name__, err, self.backoff)
                time.sleep(self.backoff)
            except Exception as err:
                self.logger.error('Unexpected %s: %s when calling HGET.',
                                  type(err).__name__, err)
                raise err
        return response

    def hgetall(self, rhash):
        while True:
            try:
                response = self.redis_client.hgetall(rhash)
                break
            except (ConnectionError, redis.exceptions.ConnectionError) as err:
                self.logger.warning('Encountered %s: %s when calling HGETALL. '
                                    'Retrying in %s seconds.',
                                    type(err).__name__, err, self.backoff)
                time.sleep(self.backoff)
            except Exception as err:
                # Why didn't we catch this?
                self.logger.error('Unexpected %s: %s when calling HGETALL. ',
                                  type(err).__name__, err)
                raise err
        return response

    def triage(self, key, pods):
        key_status = self.hget(key, 'status')

        if key_status not in {'new', 'done', 'failed'}:
            # is the pod processing this key alive?
            host = self.hget(key, 'identity_started')

            if not host:
                self.logger.info('Entry `%s` is malformed. %s',
                                 key, self.hgetall(key))
                return False

            try:
                pod_status = re.search(r'%s +\S+ +(\S+)' % host, pods).group(1)
            except AttributeError:  # pod not found, reset the status
                self.logger.info('Pod %s is AWOL. Resetting record %s.', host, key)
                self.hset(key, 'status', 'new')
                return True

            # the pod's still around, but is something wrong with it?
            if pod_status != 'Running':
                # we need to make sure it gets killed
                # and then reset the status of the job
                self.logger.info('Pod %s is in status `%s`.  Killing it '
                                 'and then resetting record %s.',
                                 host, pod_status, key)
                self.kill_pod(host)
                self.hset(key, 'status', 'new')
                return True

            # has the key's status been updated in the last N seconds?
            timeout_seconds = 300
            current_time = time.time()

            try:
                last_update = float(self.hget(key, 'timestamp_last_status_update'))
                seconds_since_last_update = current_time - (last_update / 1000)
            except TypeError as err:
                self.logger.info('Key %s with information %s has no '
                                 'appropriate timestamp_last_status_update '
                                 'field. %s: %s', key, self.hgetall(key),
                                 type(err).__name__, err)
                return False

            if seconds_since_last_update >= timeout_seconds:
                # This entry has not been updated in at least `timeout_seconds`
                # Assume it has died, and reset the status
                self.logger.info('Key `%s` has not been updated in %s seconds.'
                                 ' Resetting its status now.',
                                 key, seconds_since_last_update)
                self.hset(key, 'status', 'new')
                return True

        elif key_status == 'failed':  # TODO: should we restart all failures?
            # key failed, so try it again
            failure_reason = self.hget(key, 'reason')
            self.logger.info('Key %s failed due to "%s". Resetting its '
                             'status now.', key, failure_reason)
            self.hset(key, 'status', 'new')
            return True

        return False

    def triage_keys(self):
        # or, 1,000 reasons to restart a key
        repairs = 0

        # get list of all pods
        pods = self._get_pod_string(['kubectl', 'get', 'pods', '-a'])
        self.logger.debug('Found %s pods.', len(pods.splitlines()))

        for key in self.scan_iter():
            if self._redis_type(key) == 'hash':
                key_repaired = self.triage(key, pods)
                num_repaired = int(key_repaired)
                repairs += num_repaired
                self._repairs += num_repaired
                if num_repaired:
                    self.logger.info('Repaired key: `%s`.', key)

        self.logger.info('Repaired %s keys (%s total).',
                         repairs, self._repairs)
