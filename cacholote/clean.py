"""Functions to clean cache database and files."""

# Copyright 2022, European Union.
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

import datetime
import functools
import json
import posixpath
from typing import Any, Dict, Literal, Optional, Sequence, Set

import sqlalchemy
import sqlalchemy.orm
import structlog

from . import config, database, extra_encoders, utils

LOGGER = structlog.get_logger()


def _delete_cache_file(
    obj: Dict[str, Any],
    session: Optional[sqlalchemy.orm.Session] = None,
    cache_entry: Optional[database.CacheEntry] = None,
    sizes: Optional[Dict[str, int]] = None,
    dry_run: bool = False,
    logger: Optional[structlog.stdlib.BoundLogger] = None,
) -> Any:
    logger = logger or LOGGER

    if {"type", "callable", "args", "kwargs"} == set(obj) and obj["callable"] in (
        "cacholote.extra_encoders:decode_xr_dataset",
        "cacholote.extra_encoders:decode_io_object",
    ):
        sizes = sizes or {}
        cache_fs, cache_dirname = utils.get_cache_files_fs_dirname()
        cache_dirname = cache_fs.unstrip_protocol(cache_dirname)

        fs, urlpath = extra_encoders._get_fs_and_urlpath(*obj["args"][:2])
        urlpath = fs.unstrip_protocol(urlpath)

        if posixpath.dirname(urlpath) == cache_dirname:
            sizes.pop(urlpath, None)
            if session and cache_entry and not dry_run:
                logger.info("Delete cache entry", cache_entry=cache_entry)
                session.delete(cache_entry)
                database._commit_or_rollback(session)
            if not dry_run:
                with utils._Locker(fs, urlpath) as file_exists:
                    if file_exists:
                        logger.info("Delete cache file", urlpath=urlpath)
                        fs.rm(urlpath, recursive=True)

    return obj


class _Cleaner:
    def __init__(self, logger: structlog.stdlib.BoundLogger) -> None:
        self.logger = logger

        fs, dirname = utils.get_cache_files_fs_dirname()
        sizes = {fs.unstrip_protocol(path): fs.du(path) for path in fs.ls(dirname)}

        self.fs = fs
        self.dirname = dirname
        self.sizes = sizes

    @property
    def size(self) -> int:
        return sum(self.sizes.values())

    def stop_cleaning(self, maxsize: int) -> bool:
        size = self.size
        self.logger.info("Check cache files size", size=self.size)
        return size <= maxsize

    def get_unknown_files(self, lock_validity_period: Optional[float]) -> Set[str]:
        now = datetime.datetime.now()
        files_to_skip = []
        for urlpath in self.sizes:
            if urlpath.endswith(".lock"):
                delta = now - self.fs.modified(urlpath)
                if lock_validity_period is None or delta < datetime.timedelta(
                    seconds=lock_validity_period
                ):
                    files_to_skip.append(urlpath)
                    files_to_skip.append(urlpath.rsplit(".lock", 1)[0])

        unknown_sizes = {k: v for k, v in self.sizes.items() if k not in files_to_skip}
        if unknown_sizes:
            with config.get().sessionmaker() as session:
                for cache_entry in session.query(database.CacheEntry):
                    json.loads(
                        cache_entry._result_as_string,
                        object_hook=functools.partial(
                            _delete_cache_file,
                            sizes=unknown_sizes,
                            dry_run=True,
                            logger=self.logger,
                        ),
                    )
        return set(unknown_sizes)

    def delete_unknown_files(self, lock_validity_period: Optional[float]) -> None:
        for urlpath in self.get_unknown_files(lock_validity_period):
            self.sizes.pop(urlpath)
            if not self.fs.exists(urlpath):
                continue

            with utils._Locker(self.fs, urlpath, lock_validity_period) as file_exists:
                if file_exists:
                    self.logger.info("Delete unkown file", urlpath=urlpath)
                    self.fs.rm(urlpath)

    @staticmethod
    def check_tags(*args: Any) -> None:
        if None not in args:
            raise ValueError("tags_to_clean/keep are mutually exclusive.")
        for tags in args:
            if tags is not None and (
                not isinstance(tags, (list, set, tuple))
                or not all(tag is None or isinstance(tag, str) for tag in tags)
            ):
                raise TypeError(
                    "tags_to_clean/keep must be None or a sequence of str/None."
                )

    def delete_cache_files(
        self,
        maxsize: int,
        method: Literal["LRU", "LFU"],
        tags_to_clean: Optional[Sequence[Optional[str]]],
        tags_to_keep: Optional[Sequence[Optional[str]]],
    ) -> None:
        self.check_tags(tags_to_clean, tags_to_keep)

        # Filters
        filters = []
        if tags_to_keep is not None:
            filters.append(
                sqlalchemy.or_(
                    database.CacheEntry.tag.not_in(tags_to_keep),
                    database.CacheEntry.tag.is_not(None)
                    if None in tags_to_keep
                    else database.CacheEntry.tag.is_(None),
                )
            )
        elif tags_to_clean is not None:
            filters.append(
                sqlalchemy.or_(
                    database.CacheEntry.tag.in_(tags_to_clean),
                    database.CacheEntry.tag.is_(None)
                    if None in tags_to_clean
                    else database.CacheEntry.tag.is_not(None),
                )
            )

        # Sorters
        if method == "LRU":
            sorters = [database.CacheEntry.timestamp, database.CacheEntry.counter]
        elif method == "LFU":
            sorters = [database.CacheEntry.counter, database.CacheEntry.timestamp]
        else:
            raise ValueError("`method` must be 'LRU' or 'LFU'.")
        sorters.append(database.CacheEntry.expiration)

        # Clean database files
        if self.stop_cleaning(maxsize):
            return
        with config.get().sessionmaker() as session:
            for cache_entry in (
                session.query(database.CacheEntry).filter(*filters).order_by(*sorters)
            ):
                json.loads(
                    cache_entry._result_as_string,
                    object_hook=functools.partial(
                        _delete_cache_file,
                        session=session,
                        cache_entry=cache_entry,
                        sizes=self.sizes,
                        logger=self.logger,
                    ),
                )
                if self.stop_cleaning(maxsize):
                    return

        raise ValueError(
            f"Unable to clean {self.dirname!r}. Final size: {self.size!r}. Expected size: {maxsize!r}"
        )


def clean_cache_files(
    maxsize: int,
    method: Literal["LRU", "LFU"] = "LRU",
    delete_unknown_files: bool = False,
    lock_validity_period: Optional[float] = None,
    tags_to_clean: Optional[Sequence[Optional[str]]] = None,
    tags_to_keep: Optional[Sequence[Optional[str]]] = None,
    logger: Optional[structlog.stdlib.BoundLogger] = None,
) -> None:
    """Clean cache files.

    Parameters
    ----------
    maxsize: int
        Maximum total size of cache files (bytes).
    method: str, default: "LRU"
        * LRU: Last Recently Used
        * LFU: Least Frequently Used
    delete_unknown_files: bool, default: False
        Delete all files that are not registered in the cache database.
    lock_validity_period: float, optional, default: None
        Validity period of lock files in seconds. Expired locks will be deleted.
    tags_to_clean, tags_to_keep: sequence of strings/None, optional, default: None
        Tags to clean/keep. If None, delete all cache entries.
        To delete/keep untagged entries, add None in the sequence (e.g., [None, 'tag1', ...]).
        tags_to_clean and tags_to_keep are mutually exclusive.
    logger: optional
        Python object use to produce logs.
    """
    cleaner = _Cleaner(logger=logger or LOGGER)

    if delete_unknown_files:
        cleaner.delete_unknown_files(lock_validity_period)

    cleaner.delete_cache_files(
        maxsize=maxsize,
        method=method,
        tags_to_clean=tags_to_clean,
        tags_to_keep=tags_to_keep,
    )
