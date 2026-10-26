# Automated ETL Pipeline Using Azure (base)

An event-driven ETL pipeline: a CSV dropped into Azure Blob Storage automatically triggers an Azure Function
that cleans and validates it and upserts the result into Azure SQL Database.

```
 upload CSV                 Azure Function (blob trigger, + hourly timer sweep)
     │                     ┌────────────────────────────────────────────────────┐
     ▼                     │ Extract: parse CSV   Transform: clean + validate   │
 Blob Storage  ──────────► │ Load: MERGE (upsert) into Azure SQL  ──► dbo.sales │──► Azure SQL Database
 container: raw            └──────────────┬─────────────────────────────────────┘     dbo.etl_runs (audit)
                                          ├──► processed/   cleaned CSV
                                          ├──► rejected/    bad rows + reason
                                          ├──► archive/     original file (after success)
                                          └──► quarantine/  unparseable / wrong-schema files
```

## Run locally (no Azure account needed)
```bash
python local_run.py                                   # sample data -> local folder + SQLite
python -m unittest discover -s tests -t . -v          # tests
```
Local mode uses `.local/blobs/<container>/...` as the blob store and `.local/etl.db` as the database; the code
path is the same as in Azure (only the storage and database adapters differ).

## Deploy to Azure
1. Provision resources: `infra/deploy.sh` (Azure CLI: resource group, storage account with the 5 containers,
   serverless Azure SQL, Function App). **It has not been run against a live subscription**: read it, adjust
   names and region, and check the current `az` docs (for example, Function App hosting plan options change over time).
2. Deploy the code: `func azure functionapp publish <function-app-name>` (Azure Functions Core Tools).
3. Test: upload a file to the `raw` container
   (`az storage blob upload -c raw -f sample_data/sales_2026-10-24.csv -n sales_2026-10-24.csv --connection-string "<conn>"`),
   then query `SELECT * FROM dbo.sales; SELECT * FROM dbo.etl_runs;`.

To run the Azure code path from your machine: start Azurite, copy `local.settings.json.example` to
`local.settings.json`, fill in `SQL_CONNECTION_STRING` and run `func start`. The Microsoft ODBC Driver 18 for
SQL Server must be installed for `pyodbc`.

## Design notes
- **Idempotent loads**: rows are upserted on `order_id` (T-SQL `MERGE` in Azure SQL, `ON CONFLICT` in SQLite),
  so retries, re-uploads and the catch-up sweep never create duplicates. The whole file loads in one transaction.
- **Failure handling**: schema errors are quarantined (retrying cannot fix them); transient errors (e.g. database
  unavailable) raise, leaving the blob in `raw` so the Functions runtime retries the trigger and the hourly sweep
  picks up anything still left.
- **Data quality**: invalid dates, emails, numbers, quantities, prices and countries, plus in-file duplicate IDs,
  are rejected with a reason into `rejected/`; every run is recorded in `etl_runs` for monitoring.
- **Security**: the sample uses SQL authentication via an app setting for simplicity. For production use managed
  identity, Key Vault references for secrets, private endpoints, and restrict the SQL firewall.

## Azure Data Factory alternative
For larger or multi-source workloads, the same stages map onto Azure Data Factory: a Storage Event Trigger starts a
pipeline whose Copy activity (or Mapping Data Flow) loads `raw` into staging tables in Azure SQL, followed by a
stored-procedure `MERGE`. The validation rules in `etl/transform.py` would become data-flow expressions or SQL checks.

## Layout
- `etl/transform.py` parse, clean, validate (pure functions)
- `etl/storage.py` `LocalBlobStore` and `AzureBlobStore`
- `etl/load.py` `SqliteLoader` and `AzureSqlLoader` (+ audit table)
- `etl/pipeline.py` per-file orchestration and container routing
- `function_app.py` Azure Functions blob and timer triggers
- `local_run.py`, `infra/deploy.sh`, `host.json`, `local.settings.json.example`, `tests/`
