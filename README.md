# cacholote

Efficiently cache calls to functions

## Workflow for developers/contributors

For best experience create a new conda environment (e.g. DEVELOP) with Python 3.10:

```
conda create -n DEVELOP -c conda-forge python=3.10
conda activate DEVELOP
```

Before pushing to GitHub, run the following commands:

1. Update conda environment: `make conda-env-update`
1. Install this package: `pip install -e .`
1. Sync with the latest [template](https://github.com/ecmwf-projects/cookiecutter-conda-package) (optional): `make template-update`
1. Run quality assurance checks: `make qa`
1. Run tests: `make unit-tests`
1. Run the static type checker: `make type-check`
1. Build the documentation (see [Sphinx tutorial](https://www.sphinx-doc.org/en/master/tutorial/)): `make docs-build`

## Quick-start

```python
import time
import timeit
import cacholote

cacholote.config.set(cache_store_directory="path/to/cache/dir")

@cacholote.cacheable
def cached_sleep(x):
    time.sleep(x)
    return x

times = timeit.repeat(lambda: cached_sleep(10), number=1, repeat=5)
print(times)  # First execution takes about 10s, then almost 0s

assert cached_sleep(10) == 10

# Change settings using a context manager:
with cacholote.config.set(cache_store_directory="new/path/to/cache/dir"):
    cached_sleep(10)

# Show all available settings:
print(cacholote.config.SETTINGS)

# To use a custom key/value store other than diskcache (e.g., Redis/Pymemcache):
import redis
cacholote.config.set(cache_store=redis.Redis())

```

## License

```
Copyright 2019, B-Open Solutions srl.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
