import functools
import hashlib
import json
import time

import boto3
import fsspec
import heapdict
import pymemcache.client.hash

from . import decode
from . import encode


class DictStore:
    def __init__(self, max_count=10000):
        self.max_count = max_count
        self.store = heapdict.heapdict()
        self.stats = {"hit": 0, "miss": 0, "bad_input": 0, "bad_output": 0}

    def clear(self):
        self.store.clear()
        self.stats = {"hit": 0, "miss": 0, "bad_input": 0, "bad_output": 0}

    def _prune(self):
        while len(self.store) >= self.max_count:
            self.store.popitem()

    def get(self, key):
        try:
            expires, value = self.store[key]
            if expires > time.time():
                self.stats["hit"] += 1
                return value
        except KeyError:
            pass
        self.stats["miss"] += 1
        return None

    def set(self, key, value, expire=2635200):
        expires = time.time() + expire
        self._prune()
        self.store[key] = (expires, value)
        return True


class S3Store:
    def __init__(self, s3_root):
        fs = fsspec.filesystem("filecache", target_protocol="s3")
        self.store = fs.get_mapper(s3_root, check=True)
        self.stats = {"hit": 0, "miss": 0, "bad_input": 0, "bad_output": 0}

    def clear(self):
        self.store.clear()
        self.stats = {"hit": 0, "miss": 0, "bad_input": 0, "bad_output": 0}

    def get(self, key):
        try:
            expires_value_text = self.store[key]
            expires, value = json.loads(expires_value_text)
            if expires > time.time():
                self.stats["hit"] += 1
                return value
        except:
            pass
        self.stats["miss"] += 1
        return None

    def set(self, key, value, expire=2635200):
        expires = time.time() + expire
        expires_value_text = json.dumps((expires, value))
        self.store[key] = expires_value_text.encode("utf-8")
        return True


class DynamoDBStore:
    def __init__(self, table_name):
        self.table_name = table_name
        self.store = boto3.resource("dynamodb").Table(self.table_name)
        try:
            self.store.load()
        except:
            self.create_store()
        self.stats = {"hit": 0, "miss": 0, "bad_input": 0, "bad_output": 0}

    def create_store(self):
        dynamodb = boto3.resource("dynamodb")
        self.store = dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[
                {
                    'AttributeName': 'key',
                    'KeyType': 'HASH'
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'key',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            },
        )
        self.store.meta.client.get_waiter('table_exists').wait(TableName=self.table_name)

    def set(self, key, value, expire=2635200):
        expires = int(time.time()) + expire
        self.store.put_item(Item={"key": key, "expires": expires, "response": value})
        return True

    def get(self, key):
        try:
            item = self.store.get_item(Key={"key": key})
            value = item["Item"]["response"]
            expires = item["Item"]["expires"]
            if expires > time.time():
                self.stats["hit"] += 1
                return value
        except:
            pass
        self.stats["miss"] += 1
        return None

    def clear(self):
        with self.store.batch_writer() as batch:
            for item in self.store.scan()['Items']:
                batch.delete_item(Key={"key": item['key']})
        self.stats = {"hit": 0, "miss": 0, "bad_input": 0, "bad_output": 0}


class MemcacheStore:
    def __init__(self, servers=(("localhost", 11211),)):
        self.client = pymemcache.client.hash.HashClient(servers)
        self.stats = {"hit": 0, "miss": 0, "bad_input": 0, "bad_output": 0}
        self.set = self.client.set

    def clear(self):
        self.stats = {"hit": 0, "miss": 0, "bad_input": 0, "bad_output": 0}
        return self.client.flush_all()

    def get(self, key):
        value = self.client.get(key)
        if value is None:
            self.stats["miss"] += 1
        else:
            self.stats["hit"] += 1
        return value and value.decode("utf-8")


CACHE = DictStore()


def hexdigestify(text):
    hash_req = hashlib.sha3_224(text.encode())
    return hash_req.hexdigest()


def cacheable(filecache_root=".", cache_store=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal cache_store
            cache_store = cache_store or CACHE
            try:
                call_json = encode.dumps_python_call(
                    func, *args, _filecache_root=filecache_root, **kwargs
                )
            except TypeError:
                cache_store.stats["bad_input"] += 1
                return func(*args, **kwargs)

            hexdigest = hexdigestify(call_json)
            cached = cache_store.get(hexdigest)
            if cached is None:
                result = func(*args, **kwargs)
                try:
                    cached = encode.dumps(result, filecache_root=filecache_root)
                    cache_store.set(hexdigest, cached)
                except Exception:
                    cache_store.stats["bad_output"] += 1
                    return result
            return decode.loads(cached)

        return wrapper

    return decorator
