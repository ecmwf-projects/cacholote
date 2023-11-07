
    1. given access to an object storage bucket the system provides a permanent / transient
    mapping between requests and results

    2. a python function call is mapped to a number of object stored in the bucket, namely:
    a JSON document representing a request and a number of objects representing big binary types

    3. a cache manager identifies requests already executed and returns the permanent / transient
    result already computed unless it is expired, it also update the access to all the objects

    4. every object has an expiry datetime and a last accessed datetime

    5. a batch process removes expired objects
