import datetime
import json

from . import config, utils


def cache_files_cleaner(maxsize: int) -> None:
    fs = utils.get_cache_files_fs()
    cache_dir = utils.get_cache_files_directory()
    if fs.du(cache_dir) <= maxsize:
        return

    paths = []
    keys = []
    atimes = []
    for key in config.SETTINGS["cache_store"]:
        obj_dict = json.loads(config.SETTINGS["cache_store"][key])
        path = obj_dict.get("file:local_path")
        if path and fs.exists(path):
            paths.append(path)
            keys.append(key)
            try:
                atime = datetime.datetime.fromisoformat(obj_dict["info"]["atime"])
            except KeyError:
                atime = fs.modified(path)
            atimes.append(atime)

    # Sort by atime and clean
    for _, path, key in sorted(zip(atimes, paths, keys)):
        if fs.du(cache_dir, total=True) <= maxsize:
            break
        fs.rm(path, recursive=True)
        del config.SETTINGS["cache_store"][key]
