import logging
import subprocess
import time
import sys
import re
import os

from redis import StrictRedis
from redis.exceptions import ConnectionError

class RedisJanitor():
    def __init__(self):
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
        self._logger = logging.getLogger('autoscaler')
        self._logger.setLevel(logging.DEBUG)
        # Send logs to stdout so they can be read via Kubernetes.
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        sh.setFormatter(formatter)
        self._logger.addHandler(sh)
        # Also send logs to a file for later inspection.
        fh = logging.FileHandler('autoscaler.log')
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self._logger.addHandler(fh)

    def kill_pod(self, host):
        # delete pod
        subprocess.run(["kubectl","delete","pods",host])
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
            except ConnectionError:
                # For some reason, we're unable to connect to Redis right now.
                # Keep trying until we can.
                self._logger.warn("Trouble connecting to Redis. Retrying.")
                time.sleep(5)
    
    def redis_get_keys(self):
        while True:
            try:
                keys = self.redis_client.keys()
                break
            except ConnectionError:
                # For some reason, we're unable to connect to Redis right now.
                # Keep trying until we can.
                self._logger.warn("Trouble connecting to Redis. Retrying.")
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
                self._logger.warn("Trouble connecting to Redis. Retrying.")
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
                self._logger.warn("Trouble connecting to Redis. Retrying.")
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
                self._logger.warn("Trouble connecting to Redis. Retrying.")
                time.sleep(5)
        return key_values

    def get_pod_string(self):
        pods = subprocess.check_output(["kubectl","get","pods","-a"]).__str__()
        return pods

    def triage_keys(self):
        # get list of all pods
        pods = self.get_pod_string()
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
                elif key_status == "failed":
                    # key failed, so try it again
                    self._logger.debug("Key " + key + " failed, so it's "
                            + "being retried.")
                    self.redis_reset_status(key)
                    continue
                else:
                    continue
            else:
                continue
            # deal with health of host pod
            try:
                re_search_string = host + " +\S+ +(\S+)"
            except TypeError:
                key_info = self.redis_hgetall(key)
                self._logger.warn("This key is broked: " + key + ". " + 
                        "Resetting record's status.")
                self._logger.warn(key_info)
                self.redis_reset_status(key)
                continue
            try:
                pod_status = re.search(re_search_string,pods).group(1)
            except AttributeError:
                # no record of the pod was found
                # reset this job's status
                self._logger.debug("Pod " + host + " is awol. " +
                        "Resetting record " + key + ".")
                self.redis_reset_status(key)
                continue
            if pod_status != "Running":
                # the pod's still around, but something's wrong with it
                # we need to make sure it gets killed
                # and then reset the status of the job
                self._logger.debug("Pod " + host + " is in status " + 
                        pod_status + ". " + 
                        "Killing it and then resetting record " + key + ".")
                self.kill_pod(host)
                self.redis_reset_status(key)
            else:
                # looks like everything checked out for this record
                self._logger.debug("No problems with " + key + ".")

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
