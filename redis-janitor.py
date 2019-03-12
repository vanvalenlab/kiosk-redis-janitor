import logging
import subprocess
import time
import sys
import re
import os

from redis import StrictRedis
from redis.exceptions import ConnectionError
from subprocess import CalledProcessError


class RedisJanitor():
    def __init__(self):
        # configure variables
        self._repairs = 0

        # configure logger
        self._configure_logger()

        # establish Redis connection
        REDIS_HOST = os.environ['REDIS_HOST']
        REDIS_PORT = os.environ['REDIS_PORT']
        self.redis_client = StrictRedis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            charset='utf-8')

    def _configure_logger(self):
        self._logger = logging.getLogger('redis-janitor')
        self._logger.setLevel(logging.DEBUG)
        # Send logs to stdout so they can be read via Kubernetes.
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        sh.setFormatter(formatter)
        self._logger.addHandler(sh)
        # Also send logs to a file for later inspection.
        fh = logging.FileHandler('redis-janitor.log')
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self._logger.addHandler(fh)

    def _make_kubectl_call(self, parameter_list):
        while True:
            try:
                subprocess.run(parameter_list)
                break
            except CalledProcessError as err:
                # For some reason, we can't execute this command right now.
                # Keep trying until we can.
                self._logger.warn("Trouble executing subprocess command " + 
                        "using parameters %s. Retrying. %s: %s", 
                        parameters_list, type(err).__name__, err)
                time.sleep(5)
    
    def _get_pod_string(self, parameter_list):
        while True:
            try:
                pods_info = subprocess.check_output(parameter_list)
                pods = pods_info.__str__()
                break
            except CalledProcessError as err:
                # For some reason, we can't execute this command right now.
                # Keep trying until we can.
                self._logger.warn("Trouble executing subprocess command " + 
                        "using parameters %s. Retrying. %s: %s", 
                        parameters_list, type(err).__name__, err)
                time.sleep(5)
        return pods

    def kill_pod(self, host):
        # delete pod
        parameter_list = ["kubectl", "delete", "pods", host]
        self._make_kubectl_call(parameter_list)
        # wait until it has terminated
        while True:
            pods = self.get_pod_string()
            re_search_string = host + " +\S+ +(\S+)"
            try:
                pod_status = re.search(re_search_string,pods_str).group(1)
            except AttributeError:
                # pod no longer exists
                break
            self._logger.debug("Trying to delete pod " + host + 
                    ", but it's not gone yet.")
            time.sleep(5)

    def redis_reset_status(self, key):
        #while True:
        #    try:
        #        self.redis_client.hdel(key,"reason")
        #        break
        #    except ConnectionError:
        #        # For some reason, we're unable to connect to Redis right now.
        #        # Keep trying until we can.
        #        self._logger.warn("Trouble connecting to Redis. Retrying.")
        #        time.sleep(5)
        while True:
            try:
                self.redis_client.hset(key,"status","new")
                break
            except ConnectionError as err:
                # For some reason, we're unable to connect to Redis right now.
                # Keep trying until we can.
                self._logger.warn("Trouble connecting to Redis. Retrying. " +
                        "%s: %s", type(err).__name__, err)
                time.sleep(5)
    
    def redis_get_keys(self):
        while True:
            try:
                keys = self.redis_client.keys()
                break
            except ConnectionError as err:
                # For some reason, we're unable to connect to Redis right now.
                # Keep trying until we can.
                self._logger.warn("Trouble connecting to Redis. Retrying. " +
                        "%s: %s", type(err).__name__, err)
                time.sleep(5)
        return keys

    def redis_get_key_type(self, key):
        while True:
            try:
                key_type = self.redis_client.type(key)
                break
            except ConnectionError:
                # For some reason, we're unable to connect to Redis right now.
                # Keep trying until we can.
                self._logger.warn("Trouble connecting to Redis. Retrying. " +
                        "%s: %s", type(err).__name__, err)
                time.sleep(5)
        return key_type

    def redis_hget(self, key, field):
        while True:
            try:
                key_value = self.redis_client.hget(key, field)
                break
            except ConnectionError:
                # For some reason, we're unable to connect to Redis right now.
                # Keep trying until we can.
                self._logger.warn("Trouble connecting to Redis. Retrying. " +
                        "%s: %s", type(err).__name__, err)
                time.sleep(5)
        return key_value

    def redis_hgetall(self, key):
        while True:
            try:
                key_values = self.redis_client.hgetall(key)
                break
            except ConnectionError:
                # For some reason, we're unable to connect to Redis right now.
                # Keep trying until we can.
                self._logger.warn("Trouble connecting to Redis. Retrying. " +
                        "%s: %s", type(err).__name__, err)
                time.sleep(5)
        return key_values

    def triage_keys(self):
        # or, 1,000 reasons to restart a key
        repairs = 0
        # get list of all pods
        parameter_list = ["kubectl","get","pods","-a"]
        pods = self._get_pod_string(parameter_list)
        self._logger.debug("Got list of pods.")
        endpoint_hashes = ["new", "done", "failed"]
        keys = self.redis_get_keys()
        self._logger.debug("Got all Redis keys.")
        for key in keys:
            # get name of host redis-consumer pod
            key_type = self.redis_get_key_type(key)
            if key_type == 'hash':
                key_status = self.redis_hget(key, 'status')
                if key_status not in endpoint_hashes:
                    host = self.redis_hget(key, 'hostname')
                    # is the pod processing this key alive?
                    try:
                        pod_status = re.search(re_search_string,pods).group(1)
                    except AttributeError:
                        # no record of the pod was found
                        # reset this job's status
                        self._logger.debug("Pod " + host + " is awol. " +
                                "Resetting record " + key + ".")
                        self.redis_reset_status(key)
                        repairs = repairs + 1
                        self._repairs = self._repairs + 1
                        continue
                    # the pod's still around, but is something wrong with it?
                    if pod_status != "Running":
                        # we need to make sure it gets killed
                        # and then reset the status of the job
                        self._logger.debug("Pod " + host + " is in status " +
                                pod_status + ". " +
                                "Killing it and then resetting record " +
                                key + ".")
                        self.kill_pod(host)
                        self.redis_reset_status(key)
                        repairs = repairs + 1
                        self._repairs = self._repairs + 1
                        continue
                    # has the key's status been updated in the last N seconds?
                    timeout_seconds = 300
                    current_time = time.time() * 1000
                    last_update = self.redis_hget(key,
                            'timestamp_last_status_update')
                    seconds_since_last_update = \
                            (current_time - last_update) / 1000
                    if seconds_since_last_update >= timeout_seconds:
                        # It has been more than (timeout_seconds) seconds
                        # since this entry was updated.
                        # We are assuming it's dead or something.
                        self._logger.debug("Key " + key + " has not had its " +
                                "status updated in " + str(timeout_seconds/60)
                                + " minutes. Resetting key status now.")
                        self.redis_reset_status(key)
                        repairs = repairs + 1
                        self._repairs = self._repairs + 1
                        continue
                elif key_status == "failed":
                    # key failed, so try it again
                    self._logger.debug("Key " + key + " failed, so it's "
                            + "being retried.")
                    self.redis_reset_status(key)
                    repairs = repairs + 1
                    self._repairs = self._repairs + 1
                    continue
        self._logger.info("Keys repaired this loop: %s", repairs)
        self._logger.info("Keys repaired over all loops: %s", self._repairs)
        self._logger.info("")


    def triage_keys_loop(self):
        self._logger.debug("Entering key repair loop.")
        while True:
            self.triage_keys()
            self._logger.debug("Sleeping for 20 seconds.")
            time.sleep(20)
            self._logger.debug("")

if __name__=='__main__':
    rj = RedisJanitor()
    rj.triage_keys_loop()
