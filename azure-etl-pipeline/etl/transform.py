"""Extract + Transform: parse the CSV, clean and validate rows. Pure functions, no I/O."""
import csv
import io
import re
from datetime import datetime

REQUIRED_COLUMNS = ["order_id", "order_date", "customer_email", "product", "quantity", "unit_price", "country"]
EMAIL_RE = re.compile(r"^[\w.+-]+@[\w-]+\.[\w.-]+$")
DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y")
COUNTRIES = {
    "INDIA": "IN", "IN": "IN",
    "USA": "US", "US": "US", "UNITED STATES": "US",
    "UK": "GB", "GB": "GB", "UNITED KINGDOM": "GB",
    "UAE": "AE", "AE": "AE", "SINGAPORE": "SG", "SG": "SG",
}


class SchemaError(ValueError):
    """The file cannot be processed at all (not CSV / missing required columns)."""


def parse_csv(data: bytes) -> list[dict]:
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as e:
        raise SchemaError(f"file is not valid UTF-8: {e}") from e
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise SchemaError("file is empty")
    reader.fieldnames = [h.strip().lower() for h in reader.fieldnames]
    missing = [c for c in REQUIRED_COLUMNS if c not in reader.fieldnames]
    if missing:
        raise SchemaError(f"missing required columns: {missing}")
    return [{k: (v or "").strip() for k, v in row.items() if k is not None} for row in reader]


def _parse_date(value: str) -> str | None:
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def clean_row(r: dict) -> tuple[dict | None, str | None]:
    """Return (clean_row, None) or (None, rejection_reason)."""
    if not r.get("order_id"):
        return None, "MISSING_ORDER_ID"
    date = _parse_date(r.get("order_date", ""))
    if date is None:
        return None, "INVALID_DATE"
    email = r.get("customer_email", "").lower()
    if not EMAIL_RE.match(email):
        return None, "INVALID_EMAIL"
    try:
        qty = int(r["quantity"])
        price = round(float(r["unit_price"].replace(",", "")), 2)
    except (ValueError, KeyError):
        return None, "INVALID_NUMBER"
    if qty <= 0:
        return None, "INVALID_QUANTITY"
    if price <= 0:
        return None, "INVALID_PRICE"
    country = COUNTRIES.get(r.get("country", "").upper())
    if country is None:
        return None, "UNKNOWN_COUNTRY"
    return {
        "order_id": r["order_id"], "order_date": date, "customer_email": email,
        "product": r.get("product", "").strip().title(), "quantity": qty, "unit_price": price,
        "total_amount": round(qty * price, 2), "country": country,
    }, None


def transform(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    """Return (good_rows, rejected_rows). Rejected rows keep their original fields + `_reason`."""
    good, bad, seen = [], [], set()
    for r in rows:
        clean, reason = clean_row(r)
        if clean and clean["order_id"] in seen:
            clean, reason = None, "DUPLICATE_IN_FILE"
        if clean:
            seen.add(clean["order_id"])
            good.append(clean)
        else:
            bad.append({**r, "_reason": reason})
    return good, bad


def to_csv_bytes(rows: list[dict], columns: list[str] | None = None) -> bytes:
    if not rows:
        return b""
    cols = columns or list(rows[0].keys())
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue().encode("utf-8")
