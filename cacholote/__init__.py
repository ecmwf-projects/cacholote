"""Efficiently cache calls to functions."""

# Copyright 2019, B-Open Solutions srl.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from . import config, extra_encoders, extra_stores
from .cache import cacheable
from .decode import loads, object_hook
from .encode import dumps, filecache_default

try:
    # NOTE: the `version.py` file must not be present in the git repository
    #   as it is generated by setuptools at install time
    from .version import __version__
except ImportError:  # pragma: no cover
    # Local copy or not installed with setuptools
    __version__ = "999"

extra_encoders.register_all()

__all__ = [
    "__version__",
    "cacheable",
    "config",
    "dumps",
    "extra_encoders",
    "extra_stores",
    "filecache_default",
    "loads",
    "object_hook",
]