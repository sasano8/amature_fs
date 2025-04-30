"""
Large-file catalogs enabled by cooperative exclusivity.
"""

blueprint = {
    "rules": {
        "files": {"token": "token.json"},
        "dirs": {
            "completed": {
                "data_dir": "completed/data",
                "meta_dir": "completed/meta",
                "doc_dir": "completed/doc",
                # "allow_subdirectories": True
            },
            "processing": {
                "data_dir": "processing/data",
                "meta_dir": "processing/meta",
                "doc_dir": "processing/doc",
                # "allow_subdirectories": True
            },
            "chunked": {"data_dir": "chunked/data", "meta_dir": "chunked/meta"},
        },
        "system": {
            "default_block_size": 1024 * 1024 * 32,
            "default_hash_algorithm": "sha256",
            "multihash_format": True,
            "chunked_enabled": True,
            "prefer_chunked_read": True,
        },
    }
}

from pydantic import BaseModel


class SystemBluePrint(BaseModel):
    default_block_size: int = 1024 * 1024 * 32
    default_hash_algorithm: str = "sha256"


class FilesBluePrint(BaseModel):
    token: str = "token.json"


class DirsBluePrint(BaseModel):
    data_dir: str
    meta_dir: str
    doc_dir: str = ""


class BluePrintConfig(BaseModel):
    system: SystemBluePrint = SystemBluePrint()
    dirs: dict[str, DirsBluePrint] = {}

    @classmethod
    def get_default(cls):
        return cls.model_validate(
            {
                "system": {
                    "default_block_size": 1024 * 1024 * 32,
                    "default_hash_algorithm": "sha256",
                    # "multihash_format": True,
                    # "chunked_enabled": True,
                    # "prefer_chunked_read": True
                },
                "files": {"token": "token.json"},
                "dirs": {
                    "completed": {
                        "data_dir": "completed/data",
                        "meta_dir": "completed/meta",
                        "doc_dir": "completed/doc",
                        # "allow_subdirectories": True
                    },
                    "processing": {
                        "data_dir": "processing/data",
                        "meta_dir": "processing/meta",
                        "doc_dir": "processing/doc",
                        # "allow_subdirectories": True
                    },
                    "chunked": {
                        "data_dir": "chunked/data",
                        "meta_dir": "chunked/meta",
                        "doc_dir": "chunked/doc",
                    },
                },
            }
        )


class ChunksMetaData(BaseModel):
    block_size: int = 1024 * 1024 * 32
    block_hashes: list = []
    cumulative_hashes: list = []


class SystemMetaData(BaseModel):
    size: int | None = None
    hash: str | None = None
    chunks: dict = ChunksMetaData()


class MetaData(BaseModel):
    system: SystemMetaData = SystemMetaData()
    user: dict = {}
