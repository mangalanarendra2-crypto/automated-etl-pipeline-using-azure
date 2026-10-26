"""Azure Functions entry points (Python v2 programming model).

blob_trigger : fires when a file lands in the `raw` container -> runs the ETL for that file.
timer_trigger: hourly catch-up sweep of `raw` for anything missed or left over after a failure.
"""
import logging

import azure.functions as func

from etl.config import build
from etl.pipeline import RAW, process_all, process_blob

app = func.FunctionApp()


@app.blob_trigger(arg_name="blob", path=f"{RAW}/{{name}}", connection="AzureWebJobsStorage")
def on_new_file(blob: func.InputStream) -> None:
    name = blob.name.split("/", 1)[1]  # blob.name is "raw/<file>"
    logging.info("New blob %s (%s bytes)", name, blob.length)
    store, loader = build()
    loader.ensure_schema()
    result = process_blob(name, store, loader)  # exceptions propagate -> runtime retries, then poison queue
    logging.info("Result: %s", result)


@app.timer_trigger(schedule="0 0 * * * *", arg_name="timer", run_on_startup=False)
def hourly_sweep(timer: func.TimerRequest) -> None:
    store, loader = build()
    results = process_all(store, loader)
    logging.info("Sweep processed %d blob(s): %s", len(results), [(r["blob_name"], r["status"]) for r in results])
