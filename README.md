# cacholote

Efficiently cache calls to functions

## Quick Start

```python
>>> import cacholote
>>> cacholote.config.set(cache_db_urlpath="sqlite://")
<cacholote.config.set ...

>>> @cacholote.cacheable
... def now():
...     import datetime
...     return datetime.datetime.now()

>>> now() == now()
True

>>> with cacholote.config.set(use_cache=False):
...     now() == now()
False

```

### Cache files

```python
>>> import cacholote

>>> import tempfile
>>> tmpdir = tempfile.TemporaryDirectory().name
>>> cacholote.config.set(
...     cache_db_urlpath="sqlite://",
...     cache_files_urlpath=tmpdir,
... )
<cacholote.config.set ...

>>> cached_open = cacholote.cacheable(open)
>>> cached_file = cached_open("README.md")
>>> cached_file.name.startswith(tmpdir)
True

>>> import filecmp
>>> filecmp.cmp("README.md", cached_file.name)
True

```

### Cache Xarray objects

```python
>>> import cacholote

>>> import pytest
>>> xr = pytest.importorskip("xarray")

>>> import tempfile
>>> tmpdir = tempfile.TemporaryDirectory().name
>>> cacholote.config.set(
...     cache_db_urlpath="sqlite://",
...     cache_files_urlpath=tmpdir,
... )
<cacholote.config.set ...

>>> @cacholote.cacheable
... def dataset_from_dict(ds_dict):
...     return xr.Dataset(ds_dict)

>>> ds = dataset_from_dict({"foo": 0})
>>> ds
<xarray.Dataset> Size: 8B
Dimensions:  ()
Data variables:
    foo      int64 ...

>>> ds.encoding["source"].startswith(tmpdir)
True

```

## Configuration

Configuration settings can be accessed using `cacholote.config.get()` and modified using `cacholote.config.set(**kwargs)`. It is possible to use `cacholote.config.set` either as a context manager, or to configure global settings. See `help(cacholote.config.set)`.

Defaults are controlled by environment variables and dotenv files. See `help(cacholote.config.reset)`.

## Workflow for developers/contributors

For best experience create a new conda environment (e.g. DEVELOP) with Python 3.11:

```
conda create -n DEVELOP -c conda-forge python=3.11
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

### Instructions for database updating

In case of database structure upgrade, developers must follow these steps:

1. Update the new database structure modifying [/cacholote/database.py](/cacholote/database.py), using
   [SQLAlchemy ORM technologies](https://docs.sqlalchemy.org/en/latest/orm/)
1. Execute from the cacholote work folder:
   ```
   alembic revision -m "message about the db modification"
   ```
1. The last command will create a new python file inside [/alembic/versions](/alembic/versions). Fill the `upgrade`
   function with the operations that must be executed to migrate the database from the old structure to the new one.
   Keep in mind both DDL (structure modification) and DML (data modification) instructions. For reference,
   use https://alembic.sqlalchemy.org/en/latest/ops.html#ops.
   Similarly, do the same with the `downgrade` function.
1. Commit and push the modifications and the new file.

For details about the alembic migration tool, see the [Alembic tutorial](https://alembic.sqlalchemy.org/en/latest/tutorial.html).

## License

```
Copyright 2019, B-Open Solutions srl.
Copyright 2022, European Union.

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
