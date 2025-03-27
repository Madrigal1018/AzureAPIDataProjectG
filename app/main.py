# main.py
from fastapi import FastAPI, HTTPException
from .models import BatchData
from .crud import insert_batch, ingest_from_blob
from backup import backup_table, restore_table, backup_all_tables_to_blob, restore_all_tables_from_blob
import logging
from .utils import upload_logs_to_blob
from .report_queries import get_hiring_summary, get_top_departments_by_hiring


app = FastAPI()

@app.post("/upload/from-blob")
def upload_from_blob():
    summary = ingest_from_blob()
    return {"message": "Ingestion from blob completed", "summary": summary}


@app.post("/upload")
async def upload_data(batch: BatchData):
    try:
        insert_batch("departments", batch.departments, ["id", "department"])
        insert_batch("jobs", batch.jobs, ["id", "job"])
        insert_batch("hired_employees", batch.hired_employees, ["id", "name", "datetime", "department_id", "job_id"])
        return {"message": "Data inserted successfully"}
    except Exception as e:
        logging.error(f"Upload failed: {e}")
        raise HTTPException(status_code=400, detail="Insert error")
    

@app.post("/backup/all")
def backup_all_tables():
    tables = ["departments", "jobs", "hired_employees"]
    result = backup_all_tables_to_blob(tables)
    return {"backups": result}


@app.post("/restore/all")
def restore_all_tables():
    result = restore_all_tables_from_blob()
    return {"restores": result}
    

@app.post("/upload/logs")
def upload_logs():
    upload_logs_to_blob()
    return {"message": "Logs uploaded to blob successfully"}


@app.get("/hiring/summary")
def hiring_summary():
    try:
        result = get_hiring_summary()
        return {"summary": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.get("/hiring/top-departments")
def top_departments():
    try:
        result = get_top_departments_by_hiring()
        return {"departments": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
