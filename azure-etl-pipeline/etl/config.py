"""Environment-driven wiring.

ETL_MODE=local (default): local folder + SQLite, no cloud needed.
ETL_MODE=azure          : Azure Blob Storage + Azure SQL Database.
    AZURE_STORAGE_CONNECTION_STRING  (falls back to AzureWebJobsStorage)
    SQL_CONNECTION_STRING            ODBC connection string for Azure SQL
"""
import os

from .load import AzureSqlLoader, Loader, SqliteLoader
from .storage import AzureBlobStore, BlobStore, LocalBlobStore


def build() -> tuple[BlobStore, Loader]:
    mode = os.getenv("ETL_MODE", "local").lower()
    if mode == "azure":
        conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING") or os.environ["AzureWebJobsStorage"]
        return AzureBlobStore(conn), AzureSqlLoader()
    return LocalBlobStore(os.getenv("LOCAL_BLOB_DIR", ".local/blobs")), SqliteLoader(
        os.getenv("LOCAL_DB_PATH", ".local/etl.db"))
