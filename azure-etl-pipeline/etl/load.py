"""Load step: idempotent upserts into Azure SQL Database (or SQLite for local runs).

Both loaders implement the same interface. Upserting on order_id means a file can be
re-processed (function retry, manual re-drop, catch-up sweep) without creating duplicates.
"""
import sqlite3
from abc import ABC, abstractmethod
from contextlib import closing
from datetime import datetime, timezone

SALES_COLUMNS = ["order_id", "order_date", "customer_email", "product", "quantity",
                 "unit_price", "total_amount", "country", "source_file"]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Loader(ABC):
    @abstractmethod
    def ensure_schema(self) -> None: ...

    @abstractmethod
    def upsert_sales(self, rows: list[dict], source_file: str) -> int: ...

    @abstractmethod
    def log_run(self, record: dict) -> None: ...

    @abstractmethod
    def count_sales(self) -> int: ...


class SqliteLoader(Loader):
    def __init__(self, path: str = ".local/etl.db"):
        self.path = path

    def _connect(self):
        return closing(sqlite3.connect(self.path))

    def ensure_schema(self):
        with self._connect() as c:
            c.execute("""CREATE TABLE IF NOT EXISTS sales (
                order_id TEXT PRIMARY KEY, order_date TEXT, customer_email TEXT, product TEXT,
                quantity INTEGER, unit_price REAL, total_amount REAL, country TEXT,
                source_file TEXT, loaded_at TEXT)""")
            c.execute("""CREATE TABLE IF NOT EXISTS etl_runs (
                run_at TEXT, blob_name TEXT, status TEXT, rows_in INTEGER, rows_loaded INTEGER,
                rows_rejected INTEGER, message TEXT)""")
            c.commit()

    def upsert_sales(self, rows, source_file):
        data = [(*[r[k] for k in SALES_COLUMNS[:-1]], source_file, _now()) for r in rows]
        with self._connect() as c:
            c.executemany(
                """INSERT INTO sales VALUES (?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(order_id) DO UPDATE SET
                     order_date=excluded.order_date, customer_email=excluded.customer_email,
                     product=excluded.product, quantity=excluded.quantity, unit_price=excluded.unit_price,
                     total_amount=excluded.total_amount, country=excluded.country,
                     source_file=excluded.source_file, loaded_at=excluded.loaded_at""", data)
            c.commit()
        return len(data)

    def log_run(self, rec):
        with self._connect() as c:
            c.execute("INSERT INTO etl_runs VALUES (?,?,?,?,?,?,?)",
                      (_now(), rec["blob_name"], rec["status"], rec["rows_in"], rec["rows_loaded"],
                       rec["rows_rejected"], rec.get("message")))
            c.commit()

    def count_sales(self):
        with self._connect() as c:
            return c.execute("SELECT COUNT(*) FROM sales").fetchone()[0]

    def query(self, sql: str):
        with self._connect() as c:
            return c.execute(sql).fetchall()


class AzureSqlLoader(Loader):
    """Azure SQL Database via pyodbc. `connect` is a zero-argument callable returning a DB-API
    connection, so tests can inject a fake. Default builds one from SQL_CONNECTION_STRING."""

    CREATE_SALES = """IF OBJECT_ID('dbo.sales', 'U') IS NULL
        CREATE TABLE dbo.sales (
            order_id NVARCHAR(50) NOT NULL PRIMARY KEY, order_date DATE NOT NULL,
            customer_email NVARCHAR(255) NOT NULL, product NVARCHAR(200) NOT NULL,
            quantity INT NOT NULL, unit_price DECIMAL(12,2) NOT NULL, total_amount DECIMAL(14,2) NOT NULL,
            country CHAR(2) NOT NULL, source_file NVARCHAR(260) NOT NULL,
            loaded_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME())"""
    CREATE_RUNS = """IF OBJECT_ID('dbo.etl_runs', 'U') IS NULL
        CREATE TABLE dbo.etl_runs (
            run_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(), blob_name NVARCHAR(260),
            status NVARCHAR(30), rows_in INT, rows_loaded INT, rows_rejected INT, message NVARCHAR(1000))"""
    MERGE = """MERGE dbo.sales AS t
        USING (SELECT CAST(? AS NVARCHAR(50)) AS order_id, CAST(? AS DATE) AS order_date,
                      CAST(? AS NVARCHAR(255)) AS customer_email, CAST(? AS NVARCHAR(200)) AS product,
                      CAST(? AS INT) AS quantity, CAST(? AS DECIMAL(12,2)) AS unit_price,
                      CAST(? AS DECIMAL(14,2)) AS total_amount, CAST(? AS CHAR(2)) AS country,
                      CAST(? AS NVARCHAR(260)) AS source_file) AS s
        ON t.order_id = s.order_id
        WHEN MATCHED THEN UPDATE SET order_date = s.order_date, customer_email = s.customer_email,
             product = s.product, quantity = s.quantity, unit_price = s.unit_price,
             total_amount = s.total_amount, country = s.country, source_file = s.source_file,
             loaded_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT (order_id, order_date, customer_email, product, quantity,
             unit_price, total_amount, country, source_file)
             VALUES (s.order_id, s.order_date, s.customer_email, s.product, s.quantity,
             s.unit_price, s.total_amount, s.country, s.source_file);"""

    def __init__(self, connect=None):
        if connect is None:
            import os

            import pyodbc  # needs the Microsoft ODBC Driver 18 for SQL Server installed

            conn_str = os.environ["SQL_CONNECTION_STRING"]
            connect = lambda: pyodbc.connect(conn_str, timeout=30)  # noqa: E731
        self._connect = connect

    def _run(self, fn):
        conn = self._connect()
        try:
            result = fn(conn.cursor())
            conn.commit()  # one transaction per call: all-or-nothing
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def ensure_schema(self):
        def go(cur):
            cur.execute(self.CREATE_SALES)
            cur.execute(self.CREATE_RUNS)
        self._run(go)

    def upsert_sales(self, rows, source_file):
        params = [(*[r[k] for k in SALES_COLUMNS[:-1]], source_file) for r in rows]

        def go(cur):
            if hasattr(cur, "fast_executemany"):
                cur.fast_executemany = False  # MERGE with CASTs: keep to the safe path
            cur.executemany(self.MERGE, params)
            return len(params)
        return self._run(go) if params else 0

    def log_run(self, rec):
        self._run(lambda cur: cur.execute(
            "INSERT INTO dbo.etl_runs (blob_name, status, rows_in, rows_loaded, rows_rejected, message) "
            "VALUES (?,?,?,?,?,?)",
            rec["blob_name"], rec["status"], rec["rows_in"], rec["rows_loaded"],
            rec["rows_rejected"], (rec.get("message") or "")[:1000]))

    def count_sales(self):
        return self._run(lambda cur: cur.execute("SELECT COUNT(*) FROM dbo.sales").fetchone()[0])
