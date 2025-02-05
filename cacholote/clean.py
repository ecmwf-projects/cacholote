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
from __future__ import annotations

import collections
import datetime
import posixpath
import time
from typing import Any, Callable, Literal, Optional

import fsspec
import pydantic
import sqlalchemy as sa
import sqlalchemy.orm
from sqlalchemy import BinaryExpression, ColumnElement

from . import config, database, decode, encode, extra_encoders, utils

FILE_RESULT_KEYS = ("type", "callable", "args", "kwargs")
FILE_RESULT_CALLABLES = (
    "cacholote.extra_encoders:decode_xr_dataarray",
    "cacholote.extra_encoders:decode_xr_dataset",
    "cacholote.extra_encoders:decode_io_object",
)


def _get_files_from_cache_entry(
    cache_entry: database.CacheEntry, key: str | None
) -> dict[str, Any]:
    result = cache_entry.result
    if not isinstance(result, (list, tuple, set)):
        result = [result]

    files = {}
    for obj in result:
        if (
            isinstance(obj, dict)
            and set(FILE_RESULT_KEYS) == set(obj)
            and obj["callable"] in FILE_RESULT_CALLABLES
        ):
            fs, urlpath = extra_encoders._get_fs_and_urlpath(*obj["args"][:2])
            value = obj["args"][0]
            if key is not None:
                value = value[key]
            files[fs.unstrip_protocol(urlpath)] = value
    return files


def _remove_files(
    fs: fsspec.AbstractFileSystem,
    files: list[str],
    max_tries: int = 10,
    **kwargs: Any,
) -> None:
    assert max_tries >= 1
    if not files:
        return

    config.get().logger.info("deleting files", n_files_to_delete=len(files), **kwargs)

    n_tries = 0
    while files:
        n_tries += 1
        try:
            fs.rm(files, **kwargs)
            return
        except FileNotFoundError:
            # Another concurrent process might have deleted files
            if n_tries >= max_tries:
                raise
            files = [file for file in files if fs.exists(file)]


def _delete_cache_entries(
    session: sa.orm.Session, *cache_entries: database.CacheEntry
) -> None:
    fs, _ = utils.get_cache_files_fs_dirname()
    files_to_delete = []
    dirs_to_delete = []
    for cache_entry in cache_entries:
        session.delete(cache_entry)

        files = _get_files_from_cache_entry(cache_entry, key="type")
        for file, file_type in files.items():
            if file_type == "application/vnd+zarr":
                dirs_to_delete.append(file)
            else:
                files_to_delete.append(file)
    database._commit_or_rollback(session)

    _remove_files(fs, files_to_delete, recursive=False)
    _remove_files(fs, dirs_to_delete, recursive=True)


def delete(func_to_del: str | Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    """Delete function previously cached.

    Parameters
    ----------
    func_to_del: callable, str
        Function to delete from cache
    *args: Any
        Argument of functions to delete from cache
    **kwargs: Any
        Keyword arguments of functions to delete from cache
    """
    hexdigest = encode._hexdigestify_python_call(func_to_del, *args, **kwargs)
    with config.get().instantiated_sessionmaker() as session:
        for cache_entry in session.scalars(
            sa.select(database.CacheEntry).filter(database.CacheEntry.key == hexdigest)
        ):
            _delete_cache_entries(session, cache_entry)


class _Cleaner:
    def __init__(self, depth: int, use_database: bool) -> None:
        self.logger = config.get().logger
        self.fs, self.dirname = utils.get_cache_files_fs_dirname()

        self.urldir = self.fs.unstrip_protocol(self.dirname)

        self.logger.info("getting disk usage")
        self.file_sizes: dict[str, int] = collections.defaultdict(int)
        du = self.known_files if use_database else self.fs.du(self.dirname, total=False)
        for path, size in du.items():
            # Group dirs
            urlpath = self.fs.unstrip_protocol(path)
            parts = urlpath.replace(self.urldir, "", 1).strip("/").split("/")
            if parts:
                self.file_sizes[posixpath.join(self.urldir, *parts[:depth])] += size
        self.disk_usage = sum(self.file_sizes.values())
        self.log_disk_usage()

    def pop_file_size(self, file: str) -> int:
        size = self.file_sizes.pop(file, 0)
        self.disk_usage -= size
        return size

    def log_disk_usage(self) -> None:
        self.logger.info("check disk usage", disk_usage=self.disk_usage)

    def stop_cleaning(self, maxsize: int) -> bool:
        return self.disk_usage <= maxsize

    @property
    def known_files(self) -> dict[str, int]:
        known_files: dict[str, int] = {}
        with config.get().instantiated_sessionmaker() as session:
            for cache_entry in session.scalars(sa.select(database.CacheEntry)):
                files = _get_files_from_cache_entry(cache_entry, key="file:size")
                known_files.update(
                    {k: v for k, v in files.items() if k.startswith(self.urldir)}
                )
        return known_files

    def get_unknown_files(self, lock_validity_period: float | None) -> set[str]:
        self.logger.info("getting unknown files")

        utcnow = utils.utcnow()
        locked_files = set()
        for urlpath in self.file_sizes:
            if urlpath.endswith(".lock"):
                modified = self.fs.modified(urlpath)
                if modified.tzinfo is None:
                    modified = modified.replace(tzinfo=datetime.timezone.utc)
                delta = utcnow - modified
                if lock_validity_period is None or delta < datetime.timedelta(
                    seconds=lock_validity_period
                ):
                    locked_files.add(urlpath)
                    locked_files.add(urlpath.rsplit(".lock", 1)[0])

        return set(self.file_sizes) - locked_files - set(self.known_files)

    def delete_unknown_files(
        self, lock_validity_period: float | None, recursive: bool
    ) -> None:
        unknown_files = self.get_unknown_files(lock_validity_period)
        for urlpath in unknown_files:
            self.pop_file_size(urlpath)
        _remove_files(self.fs, list(unknown_files), recursive=recursive)
        self.log_disk_usage()

    @staticmethod
    @pydantic.validate_call
    def _get_tag_filters(
        tags_to_clean: Optional[list[Optional[str]]],
        tags_to_keep: Optional[list[Optional[str]]],
    ) -> list[sa.ColumnElement[bool]]:
        if (tags_to_clean is not None) and (tags_to_keep is not None):
            raise ValueError("tags_to_clean/keep are mutually exclusive.")

        filters = []
        if tags_to_keep is not None:
            filters.append(
                sa.or_(
                    database.CacheEntry.tag.not_in(tags_to_keep),
                    database.CacheEntry.tag.is_not(None)
                    if None in tags_to_keep
                    else database.CacheEntry.tag.is_(None),
                )
            )
        elif tags_to_clean is not None:
            filters.append(
                sa.or_(
                    database.CacheEntry.tag.in_(tags_to_clean),
                    database.CacheEntry.tag.is_(None)
                    if None in tags_to_clean
                    else database.CacheEntry.tag.is_not(None),
                )
            )
        return filters

    @staticmethod
    @pydantic.validate_call
    def _get_method_sorters(
        method: Literal["LRU", "LFU"],
    ) -> list[sa.orm.InstrumentedAttribute[Any]]:
        sorters: list[sa.orm.InstrumentedAttribute[Any]] = []
        if method == "LRU":
            sorters.extend(
                [database.CacheEntry.updated_at, database.CacheEntry.counter]
            )
        elif method == "LFU":
            sorters.extend(
                [database.CacheEntry.counter, database.CacheEntry.updated_at]
            )
        else:
            raise ValueError(f"{method=}")
        sorters.append(database.CacheEntry.expiration)
        return sorters

    def delete_cache_files(
        self,
        maxsize: int,
        method: Literal["LRU", "LFU"],
        tags_to_clean: list[str | None] | None,
        tags_to_keep: list[str | None] | None,
        batch_size: int | None,
        batch_delay: float,
    ) -> None:
        assert batch_size is None or batch_size > 0

        filters = self._get_tag_filters(tags_to_clean, tags_to_keep)
        sorters = self._get_method_sorters(method)

        files_to_delete: set[str] = set()
        stop_cleaning = self.stop_cleaning(maxsize)
        while not stop_cleaning:
            entries_to_delete: list[database.CacheEntry] = []
            self.logger.info("getting cache entries to delete")
            with config.get().instantiated_sessionmaker() as session:
                for cache_entry in session.scalars(
                    sa.select(database.CacheEntry).filter(*filters).order_by(*sorters)
                ):
                    if batch_size and len(entries_to_delete) >= batch_size:
                        break

                    files = _get_files_from_cache_entry(cache_entry, key="file:size")
                    if (
                        not self.stop_cleaning(maxsize)
                        and any(file.startswith(self.urldir) for file in files)
                    ) or (set(files) & files_to_delete):
                        entries_to_delete.append(cache_entry)
                        for file in files:
                            self.pop_file_size(file)
                            files_to_delete.add(file)
                else:
                    stop_cleaning = True

                if entries_to_delete:
                    self.logger.info(
                        "deleting cache entries",
                        n_entries_to_delete=len(entries_to_delete),
                    )
                _delete_cache_entries(session, *entries_to_delete)

            if not stop_cleaning:
                time.sleep(batch_delay)

        self.log_disk_usage()

        if not self.stop_cleaning(maxsize):
            raise ValueError(
                (
                    f"Unable to clean {self.dirname!r}."
                    f" Final disk usage: {self.disk_usage!r}."
                    f" Target disk usage: {maxsize!r}"
                )
            )


def clean_cache_files(
    maxsize: int,
    method: Literal["LRU", "LFU"] = "LRU",
    delete_unknown_files: bool = False,
    recursive: bool = False,
    lock_validity_period: float | None = None,
    tags_to_clean: list[str | None] | None = None,
    tags_to_keep: list[str | None] | None = None,
    depth: int = 1,
    use_database: bool = False,
    batch_size: int | None = None,
    batch_delay: float = 0,
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
    recursive: bool, default: False
        Whether to delete unknown directories or not
    lock_validity_period: float, optional, default: None
        Validity period of lock files in seconds. Expired locks will be deleted.
    tags_to_clean, tags_to_keep: list of strings/None, optional, default: None
        Tags to clean/keep. If None, delete all cache entries.
        To delete/keep untagged entries, add None in the list (e.g., [None, 'tag1', ...]).
        tags_to_clean and tags_to_keep are mutually exclusive.
    depth: int, default: 1
        depth for grouping cache files
    use_database: bool, default: False
        Whether to infer disk usage from the cacholote database
    batch_size: int, optional, default: None
        Number of entries to process in each batch.
        If None, all entries are processed in a single batch.
    batch_delay: float, default: 0
        Delay in seconds between processing batches.
    """
    if use_database and delete_unknown_files:
        raise ValueError(
            "'use_database' and 'delete_unknown_files' are mutually exclusive"
        )

    cleaner = _Cleaner(depth=depth, use_database=use_database)

    if delete_unknown_files:
        cleaner.delete_unknown_files(lock_validity_period, recursive)

    cleaner.delete_cache_files(
        maxsize=maxsize,
        method=method,
        tags_to_clean=tags_to_clean,
        tags_to_keep=tags_to_keep,
        batch_size=batch_size,
        batch_delay=batch_delay,
    )


def clean_invalid_cache_entries(
    check_expiration: bool = True,
    try_decode: bool = False,
    batch_size: int | None = None,
    batch_delay: float = 0,
) -> None:
    """Clean invalid cache entries.

    Parameters
    ----------
    check_expiration: bool
        Whether or not to delete expired entries
    try_decode: bool
        Whether or not to delete entries that raise DecodeError (this can be slow!)
    batch_size: int, optional, default: None
        Number of entries to process in each batch.
        If None, all entries are processed in a single batch.
    batch_delay: float, default: 0
        Delay in seconds between processing batches.
    """
    if check_expiration:
        id_stmt = (
            sa.select(database.CacheEntry.id)
            .filter(database.CacheEntry.expiration <= utils.utcnow())
            .execution_options(yield_per=batch_size)
        )
        with config.get().instantiated_sessionmaker() as session:
            partitions = list(session.scalars(id_stmt).partitions())
        for i, partition in enumerate(partitions):
            entry_stmt = sa.select(database.CacheEntry).filter(
                database.CacheEntry.id.in_(partition)
            )
            time.sleep(batch_delay if i else 0)
            with config.get().instantiated_sessionmaker() as session:
                _delete_cache_entries(session, *list(session.scalars(entry_stmt)))

    if try_decode:
        with config.get().instantiated_sessionmaker() as session:
            for cache_entry in session.scalars(sa.select(database.CacheEntry)):
                try:
                    decode.loads(cache_entry._result_as_string)
                except decode.DecodeError:
                    _delete_cache_entries(session, cache_entry)


def expire_cache_entries(
    tags: list[str] | None = None,
    before: datetime.datetime | None = None,
    after: datetime.date | None = None,
    delete: bool = False,
    batch_size: int | None = None,
    batch_delay: float = 0,
    dry_run: bool = False,
) -> int:
    now = utils.utcnow()

    filters: list[BinaryExpression[bool] | ColumnElement[bool]] = []
    if tags is not None:
        filters.append(database.CacheEntry.tag.in_(tags))
    if before is not None:
        filters.append(database.CacheEntry.created_at < before)
    if after is not None:
        filters.append(database.CacheEntry.created_at > after)
    id_stmt = (
        sa.select(database.CacheEntry.id)
        .filter(*filters)
        .execution_options(yield_per=batch_size)
    )
    with config.get().instantiated_sessionmaker() as session:
        partitions = list(session.scalars(id_stmt).partitions())

    if dry_run:
        return sum(len(partition) for partition in partitions)

    count = 0
    for i, partition in enumerate(partitions):
        entry_stmt = sa.select(database.CacheEntry).filter(
            database.CacheEntry.id.in_(partition)
        )
        time.sleep(batch_delay if i else 0)
        with config.get().instantiated_sessionmaker() as session:
            cache_entries = list(session.scalars(entry_stmt))
            count += len(cache_entries)
            if delete:
                _delete_cache_entries(session, *cache_entries)
            else:
                config.get().logger.info(
                    "expiring cache entries", n_entries_to_expire=len(cache_entries)
                )
                for cache_entry in cache_entries:
                    cache_entry.expiration = now
                database._commit_or_rollback(session)
    return count
