# backup.py
from app.utils import write_avro_file, read_avro_file, upload_file_to_blob, download_blob_to_file
from app.db import get_connection
from app.crud import insert_batch
import fastavro
import os
from typing import List
from azure.storage.blob import BlobServiceClient

def backup_table(table: str, data: list, schema: dict):
    file_path = f"backups/{table}.avro"
    with open(file_path, 'wb') as out:
        fastavro.writer(out, schema, data)

def restore_table(table: str):
    file_path = f"backups/{table}.avro"
    with open(file_path, 'rb') as f:
        reader = fastavro.reader(f)
        return list(reader)


def fetch_data_from_table(table: str):
    """Query data from Azure SQL."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table}")
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    conn.close()
    
    return [dict(zip(columns, row)) for row in rows]

def backup_table_to_blob(table: str):
    """Fetch data from SQL, write to AVRO, upload to blob."""
    records = fetch_data_from_table(table)
    local_file = write_avro_file(table, records)
    upload_file_to_blob(local_file, f"{table}.avro")
    return f"{table}.avro backed up to blob"

def restore_table_from_blob(table: str, columns: list):
    """Download AVRO from blob, insert into SQL."""
    local_file = f"backups/{table}.avro"
    download_blob_to_file(f"{table}.avro", local_file)
    records = read_avro_file(table)

    print(f"DEBUG: First record from AVRO for table '{table}':\n{records[0]}")
    
    insert_batch(table, records, columns)
    return f"{table} restored from blob"

def backup_all_tables_to_blob(tables: List[str]):
    results = []
    for table in tables:
        try:
            result = backup_table_to_blob(table)
            results.append({"table": table, "status": "success", "message": result})
        except Exception as e:
            results.append({"table": table, "status": "error", "message": str(e)})
    return results
    

def restore_all_tables_from_blob():
    """
    Restores all tables from their AVRO backups in Azure Blob.
    Returns a list of status per table.
    """
    columns_map = {
        "departments": ["id", "department"],
        "jobs": ["id", "job"],
        "hired_employees": ["id", "name", "datetime", "department_id", "job_id"]
    }

    results = []
    for table, columns in columns_map.items():
        try:
            result = restore_table_from_blob(table, columns)
            results.append({"table": table, "status": "success", "message": result})
        except Exception as e:
            results.append({"table": table, "status": "error", "message": str(e)})
    
    return results

