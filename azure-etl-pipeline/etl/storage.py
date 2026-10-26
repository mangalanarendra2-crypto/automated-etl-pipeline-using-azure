"""Blob storage abstraction: Azure Blob Storage in the cloud, a local folder for development/tests.

Containers used by the pipeline:
    raw         incoming files (this is what triggers the pipeline)
    processed   cleaned output CSVs
    rejected    rows that failed validation, with a reason column
    archive     original files after successful processing
    quarantine  files that could not be parsed at all (schema errors)
"""
from abc import ABC, abstractmethod
from pathlib import Path


class BlobStore(ABC):
    @abstractmethod
    def read(self, container: str, name: str) -> bytes: ...

    @abstractmethod
    def write(self, container: str, name: str, data: bytes) -> None: ...

    @abstractmethod
    def delete(self, container: str, name: str) -> None: ...

    @abstractmethod
    def list(self, container: str) -> list[str]: ...

    def move(self, src_container: str, dst_container: str, name: str) -> None:
        self.write(dst_container, name, self.read(src_container, name))
        self.delete(src_container, name)


class LocalBlobStore(BlobStore):
    def __init__(self, root: str = ".local/blobs"):
        self.root = Path(root)

    def _path(self, container: str, name: str) -> Path:
        return self.root / container / name

    def read(self, container, name):
        return self._path(container, name).read_bytes()

    def write(self, container, name, data):
        p = self._path(container, name)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def delete(self, container, name):
        self._path(container, name).unlink(missing_ok=True)

    def list(self, container):
        base = self.root / container
        if not base.exists():
            return []
        return sorted(p.relative_to(base).as_posix() for p in base.rglob("*") if p.is_file())


class AzureBlobStore(BlobStore):
    """Azure Blob Storage / ADLS Gen2 (blob endpoint). Needs `azure-storage-blob`."""

    def __init__(self, connection_string: str):
        from azure.storage.blob import BlobServiceClient  # imported lazily: not needed locally

        self.client = BlobServiceClient.from_connection_string(connection_string)

    def _blob(self, container, name):
        return self.client.get_blob_client(container=container, blob=name)

    def read(self, container, name):
        return self._blob(container, name).download_blob().readall()

    def write(self, container, name, data):
        self._blob(container, name).upload_blob(data, overwrite=True)

    def delete(self, container, name):
        self._blob(container, name).delete_blob(delete_snapshots="include")

    def list(self, container):
        return sorted(b.name for b in self.client.get_container_client(container).list_blobs())
