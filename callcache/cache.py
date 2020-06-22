import functools
import hashlib
import time
import typing as T

import boto3
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
            expires, value, expanded_key = self.store[key]
            if expires > time.time():
                self.stats["hit"] += 1
                return value
        except KeyError:
            pass
        self.stats["miss"] += 1
        return None

    def set(self, key, value, expire=2635200, expanded_key=None):
        expires = time.time() + expire
        self._prune()
        self.store[key] = (expires, value, expanded_key)
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
            KeySchema=[{"AttributeName": "key", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "key", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        self.store.meta.client.get_waiter("table_exists").wait(
            TableName=self.table_name
        )

    def set(self, key, value, expire=2635200, expanded_key=None):
        expires = int(time.time()) + expire
        self.store.put_item(
            Item={
                "key": key,
                "expires": expires,
                "response": value,
                "request": expanded_key,
            }
        )
        return True

    def get(self, key):
        try:
            item = self.store.get_item(Key={"key": key})
            value = item["Item"]["response"]
            expires = item["Item"]["expires"]
            if expires > time.time():
                print(type(item["Item"]["request"]), item["Item"]["request"])
                print(type(value), value)
                self.stats["hit"] += 1
                return value
        except:
            pass
        self.stats["miss"] += 1
        return None

    def clear(self):
        with self.store.batch_writer() as batch:
            for item in self.store.scan()["Items"]:
                batch.delete_item(Key={"key": item["key"]})
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


def hexdigestify(text: str) -> str:
    hash_req = hashlib.sha3_224(text.encode())
    return hash_req.hexdigest()


def cacheable(filecache_root=".", cache_store=None, version=None):
    def decorator(func: T.Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal cache_store
            cache_store = cache_store or CACHE
            try:
                call_json = encode.dumps_python_call(
                    func,
                    *args,
                    _filecache_root=filecache_root,
                    _callable_version=version,
                    **kwargs
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
                    cache_store.set(hexdigest, cached, expanded_key=call_json)
                except Exception:
                    cache_store.stats["bad_output"] += 1
                    return result
            return decode.loads(cached)

        return wrapper

    return decorator
