import os


from .models import (
    SystemBluePrint,
    FilesBluePrint,
    DirsBluePrint,
    BluePrintConfig,
    ChunksMetaData,
    SystemMetaData,
    MetaData,
    blueprint,
)


import fsspec
import json
from os import path
from contextlib import contextmanager
import hashlib
import copy
from .exceptions import RFC7807Error

CATALOG_JSON_PATH = "catalog.json"


def calculate_hash(algorithm, data) -> str:
    if algorithm not in hashlib.algorithms_available:
        raise RFC7807Error.internalservererror(
            f"Not supported hash algorithm: {algorithm}"
        )

    h = getattr(hashlib, algorithm)()
    h.update(data)
    return h.hexdigest()


def get_hash_cls(algorithm):
    if algorithm not in hashlib.algorithms_available:
        raise RFC7807Error.internalservererror(
            f"Not supported hash algorithm: {algorithm}"
        )
    cls = getattr(hashlib, algorithm)
    return cls


class CatalogHelper:
    def __init__(self, catalog_config: dict):
        self._config = catalog_config

    def get_processing_meta_path(self, key):
        p = path.join(self._config["processing"]["meta_dir"], key)
        return p

    def get_completed_data_path(self, key):
        p = path.join(self._config["completed"]["data_dir"], key)
        return p

    def get_completed_meta_path(self, key):
        p = path.join(self._config["completed"]["meta_dir"], key)
        return p


class LockDir:
    def __init__(self, client: fsspec.AbstractFileSystem, path, lock_id: str):
        self._client = client
        self._path = path
        self._lock_id = lock_id

    @contextmanager
    @classmethod
    def create(cls, client, path, lock_id=None):
        import uuid

        lock_id = lock_id or str(uuid.uuid4())
        lock = cls(client, path, lock_id)
        with lock as lock:
            return lock

    def __enter__(self):
        self.lock()
        return self

    def __exit__(self, *args, **kwargs):
        self.unlock()

    def is_locked(self):
        return self._client.exists(self._path)

    def lock(self):
        if self.is_locked():
            raise RFC7807Error.resource_locked()

        self._client.mkdir(self._path, create_parents=True)

    def unlock(self):
        self._client.rm(self._path, recursive=True)

    def open(self, path): ...


class StoreBluePrint:
    @classmethod
    def get_default(self):
        return copy.deepcopy(blueprint)

    @classmethod
    def load_from_path(self, path):
        raise NotImplementedError()

    @classmethod
    def load_from_dict(self, config: dict = None):
        conf = config or self.get_default()

    def __init__(self, blueprint: dict):
        self._blueprint = blueprint

    def cleanup(self, fs: fsspec.AbstractFileSystem, token: str):
        for name in fs.ls("", detail=False):
            fs.rm(name, recursive=True)

    def clear(self, fs: fsspec.AbstractFileSystem, token: str):
        fs.rm("token.json", recursive=True)

    def init(self, fs: fsspec.AbstractFileSystem, token: str):
        if fs.exists("token.json"):
            raise RFC7807Error.internalservererror()

        for k, v in self._blueprint["rules"]["dirs"].items():
            data_dir = v.get("data_dir", "")
            meta_dir = v.get("meta_dir", "")
            doc_dir = v.get("doc_dir", "")
            fs.mkdirs(data_dir, exist_ok=True)
            fs.mkdirs(meta_dir, exist_ok=True)
            fs.mkdirs(doc_dir, exist_ok=True)

        with fs.open("token.json", "w") as f:
            json.dump(token, f)

    def get_block_size(self):
        return int(self._blueprint["rules"]["system"]["default_block_size"])

    def get_processing_data_path(self, key):
        path = os.path.join(
            self._blueprint["rules"]["dirs"]["processing"]["data_dir"], key
        )
        return path

    def get_processing_meta_path(self, key):
        path = os.path.join(
            self._blueprint["rules"]["dirs"]["processing"]["meta_dir"], key
        )
        return path

    def get_completed_data_path(self, key):
        path = os.path.join(
            self._blueprint["rules"]["dirs"]["completed"]["data_dir"], key
        )
        return path

    def get_completed_meta_path(self, key):
        path = os.path.join(
            self._blueprint["rules"]["dirs"]["completed"]["meta_dir"], key
        )
        return path

    @contextmanager
    def begin(self, fs: fsspec.AbstractFileSystem, key, usermeta: dict):
        path = self.get_processing_meta_path(key)
        if fs.exists(path):
            raise RFC7807Error.resource_locked()

        meta = MetaData(user=usermeta).model_dump()
        with fs.open(path, "w") as f:
            json.dump(meta, f)

        try:
            yield meta
            self.commit(fs, key, meta)
        except Exception as e:
            try:
                self.rollback(fs, key)
            except Exception as e2:
                raise RFC7807Error.internalservererror(
                    extensions={"errors": [e, e2]}
                ) from e2

            raise

    def commit(self, fs: fsspec.AbstractFileSystem, key, meta):
        validated = MetaData.model_validate(meta).model_dump()
        meta_path = self.get_processing_meta_path(key)
        data_path = self.get_processing_data_path(key)

        with fs.open(meta_path, "w") as f:
            json.dump(validated, f)

        fs.mv(data_path, self.get_completed_data_path(key))
        fs.mv(meta_path, self.get_completed_meta_path(key))

    def rollback(self, fs: fsspec.AbstractFileSystem, key):
        completed_data_path = self.get_completed_data_path(key)
        completed_meta_path = self.get_completed_meta_path(key)
        processing_meta_path = self.get_processing_meta_path(key)
        processing_data_path = self.get_processing_data_path(key)

        if fs.exists(completed_data_path):
            fs.rm(completed_data_path, recursive=True)

        if fs.exists(completed_meta_path):
            fs.rm(completed_meta_path, recursive=True)

        if fs.exists(processing_data_path):
            fs.rm(processing_data_path, recursive=True)

        if fs.exists(processing_meta_path):
            fs.rm(processing_meta_path, recursive=True)

    # @contextmanager
    def write_file(
        self,
        fs: fsspec.AbstractFileSystem,
        key: str,
        file,
        usermeta: dict = {},
        block_size: int = None,
    ):
        block_size = block_size or self.get_block_size()
        hashargs = usermeta.get("hash", "sha256:").split(":")
        algorithm = hashargs[0]
        algorithm = algorithm or "sha256"

        hashcls = get_hash_cls(algorithm)
        cumulative_hashe = hashcls()
        block_hashes = []
        cumulative_hashes = []
        size = 0

        path = self.get_processing_data_path(key)

        with self.begin(fs, key, usermeta) as meta:
            with fs.open(path, "wb") as f:
                while True:
                    buf = file.read(block_size)
                    if not buf:
                        break

                    f.write(buf)
                    size += len(buf)

                    hashobj = hashcls()
                    hashobj.update(buf)
                    hash = hashobj.hexdigest()
                    block_hashes.append(algorithm + ":" + hash)

                    cumulative_hashe.update(buf)
                    hash = cumulative_hashe.hexdigest()
                    cumulative_hashes.append(algorithm + ":" + hash)

                # 0 size の場合
                if not cumulative_hashes:
                    hash = cumulative_hashe.hexdigest()
                    cumulative_hashes.append(algorithm + ":" + hash)
                    block_hashes.append(algorithm + ":" + hash)

            meta["system"]["size"] = size
            meta["system"]["hash"] = cumulative_hashes[-1]
            meta["system"]["chunks"] = {
                "block_size": block_size,
                "block_hashes": block_hashes,  # ブロックごとのハッシュ
                "cumulative_hashes": cumulative_hashes,  # そのブロック時点の累計ハッシュ
            }

            size = meta["user"].get("size", None)
            size = meta["system"]["size"] if size is None else size

            hash = meta["user"].get("hash", None)
            hash = meta["system"]["hash"] if not hash else hash

            if not (size == meta["system"]["size"]):
                raise RFC7807Error.file_integrity_error("Size mismatch.")
            if not (hash == meta["system"]["hash"]):
                raise RFC7807Error.file_integrity_error("Hash mismatch.")

    def open(self, fs: fsspec.AbstractFileSystem, key: str, mode: str = "rb"):
        if mode not in {"rb", "r"}:
            raise RFC7807Error.internalservererror()

        processing_meta_path = self.get_processing_meta_path(key)
        if fs.exists(processing_meta_path):
            raise RFC7807Error.resource_locked()

        completed_data_path = self.get_completed_data_path(key)
        return fs.open(completed_data_path, mode=mode)

    def read_meta(self, fs: fsspec.AbstractFileSystem, key: str):
        processing_meta_path = self.get_processing_meta_path(key)
        if fs.exists(processing_meta_path):
            raise RFC7807Error.resource_locked()

        completed_meta_path = self.get_completed_meta_path(key)
        with fs.open(completed_meta_path, "r") as f:
            meta = json.load(f)

        return meta

    def ls(self, fs: fsspec.AbstractFileSystem, key: str = ""):
        completed_meta_path = self.get_completed_meta_path(key)
        for k in fs.ls(completed_meta_path, detail=False):
            yield k.replace("completed/meta/", "")


class MyStore:
    @classmethod
    def from_local(cls, path: str = ".cache/catalog"):
        fs, _ = fsspec.url_to_fs(f"dir::local://{path}")
        blueprint = StoreBluePrint(StoreBluePrint.get_default())

        return cls.from_fsspec(fs, blueprint)

    @classmethod
    def from_fsspec(cls, client: fsspec.AbstractFileSystem, blueprint: StoreBluePrint):
        return cls(client, blueprint)

    def __init__(self, client: fsspec.AbstractFileSystem, blueprint: StoreBluePrint):
        self._client = client
        self._blueprint = blueprint

    def clear(self, token: str = None):
        self._blueprint.clear(self._client, token)

    def cleanup(self, token: str = None):
        self._blueprint.cleanup(self._client, token)

    def init(self, token: str):
        self._blueprint.init(self._client, token)

    def write_file(self, key, file, usermeta: dict = {}):
        return self._blueprint.write_file(self._client, key, file, usermeta)

    def open(self, key, mode: str = "rb"):
        return self._blueprint.open(self._client, key, mode=mode)

    def read_meta(self, key: str):
        return self._blueprint.read_meta(self._client, key)

    def ls(self, key: str):
        return list(self._blueprint.ls(self._client, key))
