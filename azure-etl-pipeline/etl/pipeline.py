"""Orchestrates one file through extract -> transform -> load and routes blobs between containers."""
import logging

from .load import Loader
from .storage import BlobStore
from .transform import SchemaError, parse_csv, to_csv_bytes, transform

log = logging.getLogger("etl")

RAW, PROCESSED, REJECTED, ARCHIVE, QUARANTINE = "raw", "processed", "rejected", "archive", "quarantine"


def process_blob(name: str, store: BlobStore, loader: Loader) -> dict:
    """Process one blob from the `raw` container.

    - Unparseable / wrong-schema file  -> moved to `quarantine`, run logged as FAILED_SCHEMA (no retry: it can't succeed).
    - Valid file -> good rows upserted, cleaned CSV to `processed`, bad rows to `rejected`, original to `archive`.
    - Transient failure (e.g. database unavailable) -> exception propagates, blob stays in `raw`, and the
      Azure Functions runtime retries the trigger.
    """
    data = store.read(RAW, name)
    try:
        rows = parse_csv(data)
    except SchemaError as e:
        store.move(RAW, QUARANTINE, name)
        rec = {"blob_name": name, "status": "FAILED_SCHEMA", "rows_in": 0, "rows_loaded": 0,
               "rows_rejected": 0, "message": str(e)}
        loader.log_run(rec)
        log.error("%s quarantined: %s", name, e)
        return rec

    good, bad = transform(rows)
    loaded = loader.upsert_sales(good, name)  # raises on failure -> trigger retry, nothing is moved

    base = name.rsplit(".", 1)[0]
    if good:
        store.write(PROCESSED, f"{base}_clean.csv", to_csv_bytes(good))
    if bad:
        store.write(REJECTED, f"{base}_rejected.csv", to_csv_bytes(bad))
    store.move(RAW, ARCHIVE, name)

    rec = {"blob_name": name, "status": "SUCCESS", "rows_in": len(rows), "rows_loaded": loaded,
           "rows_rejected": len(bad), "message": None}
    loader.log_run(rec)
    log.info("%s: %d in, %d loaded, %d rejected", name, len(rows), loaded, len(bad))
    return rec


def process_all(store: BlobStore, loader: Loader) -> list[dict]:
    """Sweep the raw container (used by the timer trigger and the local runner)."""
    loader.ensure_schema()
    results = []
    for name in store.list(RAW):
        try:
            results.append(process_blob(name, store, loader))
        except Exception as e:  # keep sweeping; the failed blob stays in raw for the next sweep
            log.exception("failed to process %s", name)
            results.append({"blob_name": name, "status": "ERROR", "rows_in": 0, "rows_loaded": 0,
                            "rows_rejected": 0, "message": f"{type(e).__name__}: {e}"})
    return results
