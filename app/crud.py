# crud.py
import os
import logging
import pandas as pd
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from .db import get_connection
from .logger import success_logger, fail_logger

# Load environment variables
load_dotenv()

# Ingest and insert data from blob into Azure SQL
def ingest_from_blob():
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = "employeedata"

    file_config = {
        "departments.csv": (["id", "department"], "departments"),
        "jobs.csv": (["id", "job"], "jobs"),
        "hired_employees.csv": (["id", "name", "datetime", "department_id", "job_id"], "hired_employees")
    }

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    summary = {}

    for file_name, (headers, table_name) in file_config.items():
        try:
            # Download file
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
            with open(file_name, "wb") as f:
                f.write(blob_client.download_blob().readall())

            # Load and clean DataFrame
            df = pd.read_csv(file_name, header=None, names=headers)
            missing = df[df.isna().any(axis=1)]
            df.dropna(inplace=True)

            # Log skipped rows to fail logger
            for _, row in missing.iterrows():
                fail_logger.error(f"{table_name} SKIPPED: {row.to_dict()}")

            # Insert rows
            records = df.astype(object).where(pd.notnull(df), None).to_dict(orient="records")
            inserted = insert_batch(table_name, records, headers)
            summary[table_name] = {"inserted": inserted, "skipped": len(missing)}

        except Exception as e:
            logging.error(f"Error processing {file_name}: {e}")
            summary[table_name] = {"error": str(e)}

    return summary

# Insert logic used by ingestion and API

def insert_batch(table, data, columns):
    conn = get_connection()
    cursor = conn.cursor()

    placeholders = ', '.join(['?'] * len(columns))
    insert_query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"

    inserted_count = 0

    try:
        for row in data:
            try:
                # Handle both Pydantic objects and dictionaries
                values = [row[col] if isinstance(row, dict) else getattr(row, col) for col in columns]
                cursor.execute(insert_query, values)
                success_logger.info(f"{table} INSERTED: {values}")
                inserted_count += 1
            except Exception as row_error:
                fail_logger.error(f"{table} FAILED: {row} | ERROR: {row_error}")
        conn.commit()
    except Exception as e:
        logging.error(f"Insert failed for {table}: {e}")
        raise
    finally:
        conn.close()

    return inserted_count
