import datetime
import json

from . import config, utils


def cache_files_cleaner(maxsize: int, delete_unknown_files: bool = True) -> None:
    fs = utils.get_cache_files_fs()
    cache_dir = utils.get_cache_files_directory()
    if fs.du(cache_dir) <= maxsize:
        return

    paths = []
    keys = []
    atimes = []
    for key in utils.cache_store_keys_iter():
        obj_dict = json.loads(config.SETTINGS["cache_store"][key])
        path = obj_dict.get("file:local_path")
        if path and fs.exists(path):
            path = fs.unstrip_protocol(path)
            paths.append(path)
            keys.append(key)
            try:
                atime = obj_dict["info"]["atime"]
            except KeyError:
                # get time from file metadata
                atime = fs.modified(path)
            else:
                # atime stored by cacholote is a string
                atime = datetime.datetime.fromisoformat(atime)
            atimes.append(atime)

    if delete_unknown_files:
        for path in fs.ls(cache_dir):
            path = fs.unstrip_protocol(path)
            if path not in paths:
                paths.append(path)
                keys.append("")
                atimes.append(fs.modified(path))

    # Sort by atime and clean
    for _, path, key in sorted(zip(atimes, paths, keys)):
        if fs.du(cache_dir, total=True) <= maxsize:
            break
        fs.rm(path, recursive=True)
        if key:
            utils.delete_cache_store_key(key)
