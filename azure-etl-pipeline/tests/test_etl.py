import tempfile
import unittest
from pathlib import Path

from etl.load import AzureSqlLoader, SqliteLoader
from etl.pipeline import ARCHIVE, PROCESSED, QUARANTINE, RAW, REJECTED, process_all, process_blob
from etl.storage import LocalBlobStore
from etl.transform import SchemaError, clean_row, parse_csv, transform

SAMPLE = Path("sample_data")
GOOD = {"order_id": "1", "order_date": "2026-10-20", "customer_email": "A@B.com", "product": "mouse",
        "quantity": "2", "unit_price": "10.50", "country": "india"}


class TransformTests(unittest.TestCase):
    def test_clean_row(self):
        row, reason = clean_row(GOOD)
        self.assertIsNone(reason)
        self.assertEqual((row["customer_email"], row["country"], row["total_amount"], row["product"]),
                         ("a@b.com", "IN", 21.0, "Mouse"))

    def test_rejections(self):
        cases = {"order_id": ("", "MISSING_ORDER_ID"), "order_date": ("2026-13-45", "INVALID_DATE"),
                 "customer_email": ("nope", "INVALID_EMAIL"), "quantity": ("abc", "INVALID_NUMBER"),
                 "country": ("Narnia", "UNKNOWN_COUNTRY"), "unit_price": ("-1", "INVALID_PRICE")}
        for field, (value, expected) in cases.items():
            self.assertEqual(clean_row({**GOOD, field: value})[1], expected, field)
        self.assertEqual(clean_row({**GOOD, "quantity": "0"})[1], "INVALID_QUANTITY")

    def test_alternate_date_format_and_thousands_separator(self):
        row, _ = clean_row({**GOOD, "order_date": "21/10/2026", "unit_price": "3,499.00"})
        self.assertEqual((row["order_date"], row["unit_price"]), ("2026-10-21", 3499.0))

    def test_duplicates_within_file(self):
        good, bad = transform([GOOD, dict(GOOD)])
        self.assertEqual((len(good), bad[0]["_reason"]), (1, "DUPLICATE_IN_FILE"))

    def test_schema_errors(self):
        with self.assertRaises(SchemaError):
            parse_csv(b"order_id,order_date\n1,2026-01-01\n")
        with self.assertRaises(SchemaError):
            parse_csv(b"")


class LocalPipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = LocalBlobStore(f"{self.tmp.name}/blobs")
        self.loader = SqliteLoader(f"{self.tmp.name}/etl.db")
        self.loader.ensure_schema()

    def tearDown(self):
        self.tmp.cleanup()

    def drop(self, name):
        self.store.write(RAW, name, (SAMPLE / name).read_bytes())

    def test_sample_file_end_to_end(self):
        self.drop("sales_2026-10-24.csv")
        rec = process_blob("sales_2026-10-24.csv", self.store, self.loader)
        self.assertEqual((rec["status"], rec["rows_in"], rec["rows_loaded"], rec["rows_rejected"]),
                         ("SUCCESS", 11, 5, 6))
        self.assertEqual(self.loader.count_sales(), 5)
        self.assertEqual(self.store.list(RAW), [])
        self.assertEqual(self.store.list(ARCHIVE), ["sales_2026-10-24.csv"])
        self.assertEqual(self.store.list(PROCESSED), ["sales_2026-10-24_clean.csv"])
        self.assertEqual(self.store.list(REJECTED), ["sales_2026-10-24_rejected.csv"])

    def test_reprocessing_is_idempotent(self):
        for _ in range(2):
            self.drop("sales_2026-10-24.csv")
            process_blob("sales_2026-10-24.csv", self.store, self.loader)
        self.assertEqual(self.loader.count_sales(), 5)  # no duplicates after a re-drop

    def test_bad_schema_is_quarantined(self):
        self.drop("bad_schema.csv")
        rec = process_blob("bad_schema.csv", self.store, self.loader)
        self.assertEqual(rec["status"], "FAILED_SCHEMA")
        self.assertEqual(self.store.list(QUARANTINE), ["bad_schema.csv"])
        self.assertEqual(self.store.list(RAW), [])
        self.assertEqual(self.loader.count_sales(), 0)

    def test_transient_failure_keeps_blob_in_raw(self):
        self.drop("sales_2026-10-24.csv")

        class Boom(SqliteLoader):
            def upsert_sales(self, rows, source_file):
                raise ConnectionError("database unavailable")

        results = process_all(self.store, Boom(f"{self.tmp.name}/etl.db"))
        self.assertEqual(results[0]["status"], "ERROR")
        self.assertEqual(self.store.list(RAW), ["sales_2026-10-24.csv"])  # still there for retry
        self.assertEqual(self.store.list(ARCHIVE), [])


class FakeCursor:
    def __init__(self, log):
        self.log = log

    def execute(self, sql, *params):
        self.log.append(("execute", sql, params))
        return self

    def executemany(self, sql, params):
        self.log.append(("executemany", sql, list(params)))

    def fetchone(self):
        return (0,)


class FakeConn:
    def __init__(self, log):
        self.log = log

    def cursor(self):
        return FakeCursor(self.log)

    def commit(self):
        self.log.append(("commit",))

    def rollback(self):
        self.log.append(("rollback",))

    def close(self):
        self.log.append(("close",))


class AzureSqlLoaderTests(unittest.TestCase):
    """Exercises the Azure SQL code path with a fake connection (no real database)."""

    def test_upsert_uses_merge_and_commits(self):
        log = []
        loader = AzureSqlLoader(connect=lambda: FakeConn(log))
        good, _ = transform([GOOD])
        self.assertEqual(loader.upsert_sales(good, "f.csv"), 1)
        kinds = [e[0] for e in log]
        self.assertIn("executemany", kinds)
        self.assertEqual(kinds[-2:], ["commit", "close"])
        sql, params = next((e[1], e[2]) for e in log if e[0] == "executemany")
        self.assertTrue(sql.lstrip().startswith("MERGE dbo.sales"))
        self.assertEqual(params[0][0], "1")
        self.assertEqual(params[0][-1], "f.csv")

    def test_rollback_on_error(self):
        log = []

        class BadCursor(FakeCursor):
            def executemany(self, sql, params):
                raise RuntimeError("deadlock")

        class BadConn(FakeConn):
            def cursor(self):
                return BadCursor(self.log)

        loader = AzureSqlLoader(connect=lambda: BadConn(log))
        good, _ = transform([GOOD])
        with self.assertRaises(RuntimeError):
            loader.upsert_sales(good, "f.csv")
        self.assertIn(("rollback",), log)
        self.assertNotIn(("commit",), log)


if __name__ == "__main__":
    unittest.main()
