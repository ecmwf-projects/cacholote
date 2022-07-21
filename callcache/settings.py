"""
Handle global settings.

SETTINGS can be imported elsewhere to use global settings.
"""

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

from types import MappingProxyType, TracebackType
from typing import Any, Dict, Optional, Type

import diskcache

_SETTINGS: Dict[str, Any] = {
    "cache": diskcache.Cache(disk=diskcache.JSONDisk, statistics=True),
}
# Immutable settings to be used by other modules
SETTINGS = MappingProxyType(_SETTINGS)


class config:
    # TODO: Add docstring
    def __init__(self, **kwargs: Any):
        wrong_keys = set(kwargs) - set(_SETTINGS)
        if wrong_keys:
            raise ValueError(f"The following settings do NOT exist: {wrong_keys!r}")

        self._old = {key: _SETTINGS[key] for key in kwargs}
        _SETTINGS.update(kwargs)

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        _SETTINGS.update(self._old)
