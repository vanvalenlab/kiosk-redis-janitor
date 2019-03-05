from decouple import config
from redis import StrictRedis
from redis.exceptions import ConnectionError
import logging
import subprocess
import time
import sys
import re

class RedisJanitor():
    def __init__(self):
        DEBUG = config('DEBUG', cast=bool, default=False)
        self._initialize_logger(DEBUG)

        self._logger = logging.getLogger("redis-janitor.log")

        REDIS_HOST = config('REDIS_HOST', default='redis-master')
        REDIS_PORT = config('REDIS_PORT', default=6379, cast=int)
        self.redis_client = StrictRedis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            charset='utf-8')

    def _initialize_logger(self, debug_mode=False):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter(
                '%(asctime)s -- [%(levelname)s]:[%(name)s]: %(message)s')
        console = logging.StreamHandler(stream=sys.stdout)
        console.setFormatter(formatter)

        if debug_mode:
            console.setLevel(logging.DEBUG)
        else:
            console.setLevel(logging.INFO)

        self.logger.addHandler(console)

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
        
        endpoint_hashes =["new", "done", "failed"]
        keys = self.redis_get_keys()
        for key in keys:
            # get name of host redis-consumer pod
            key_type = self.red_get_key_type(key)
            if key_type == 'hash':
                key_status = self.redis_hget(key, 'status')
                if key_status not in endpoint_hashes:
                    host = self.redis_hget(key, 'hostname')
                else:
                    continue
            else:
                continue
            # verify host pod is running
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
                pass

    def triage_keys_loop(self):
        while True:
            self.triage_keys()
            self._logger.debug("")
            self._logger.debug("Sleeping for 20 seconds.")
            time.sleep(20)
            self._logger.debug("Exiting sleep.")
            self._logger.debug("")


if __name__=='__main__':
    rj = RedisJanitor()
    rj.triage_keys_loop()
