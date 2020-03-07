
User requirements:

    1. express how to call a generic Python callable via JSON, including passing inputs and outputs

        1. enhance Python JSON serialisation / deserialisation to reference generic
        Python objects in the modules namespace

        2. enhance Python JSON deserialisation to express and execute a function call

        3. enhance Python JSON serialisation / deserialisation with complex instances
        that reference data by URL (including a local path)

    2. keep a cache of JSON documents for calls and corresponding outputs for callables that request it

    3. if output URLs are not valid the cache is invalidated and the function re-run

    4. if input URLs are not valid the cache is invalidated and an error is raised

    5. the cache can be shared among processes and possibly hosts


Accepted limitations:

    1. it is OK to be Python centric

    2. users that create JSON documents have enough rights to make the corresponding calls -
    no need for extra security

    3.
