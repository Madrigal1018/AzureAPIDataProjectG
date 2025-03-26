import fastavro
import os
from datetime import datetime
from typing import List, Dict
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from .logger import get_log_paths


# Load .env file
load_dotenv()


# Load from environment or define manually
BLOB_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER = os.getenv("BLOB_CONTAINER_NAME", "backups")
BLOB_CONTAINERLOGS = os.getenv("BLOB_CONTAINER_NAME", "logsinsertions")

blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
container_client = blob_service.get_container_client(BLOB_CONTAINER)
container_client2 = blob_service.get_container_client(BLOB_CONTAINERLOGS)


def ensure_backup_dir():
    """Ensure the 'backups' folder exists."""
    if not os.path.exists("backups"):
        os.makedirs("backups")


def get_avro_schema_from_records(records: List[Dict]) -> Dict:
    """
    Generate a minimal AVRO schema from a list of dictionaries.
    Assumes all records have the same keys and non-null types.
    """
    if not records:
        raise ValueError("No records provided for schema generation.")

    first = records[0]
    fields = []

    for key, value in first.items():
        field_type = python_to_avro_type(type(value))
        fields.append({"name": key, "type": field_type})

    return {
        "doc": "Auto-generated schema",
        "name": "AutoSchema",
        "namespace": "example.avro",
        "type": "record",
        "fields": fields
    }


def python_to_avro_type(py_type):
    """Convert Python type to basic AVRO type."""
    if py_type in [int]:
        return "int"
    elif py_type in [float]:
        return "float"
    elif py_type in [bool]:
        return "boolean"
    elif py_type in [datetime]:
        return "string"  # Could be logicalType later
    else:
        return "string"  # Default fallback


def write_avro_file(table_name: str, records: List[Dict]):
    """Write records to AVRO file with dynamic schema."""
    ensure_backup_dir()
    file_path = f"backups/{table_name}.avro"

    schema = get_avro_schema_from_records(records)

    with open(file_path, 'wb') as out:
        fastavro.writer(out, schema, records)

    return file_path


def read_avro_file(table_name: str) -> List[Dict]:
    """Read an AVRO file and return list of records."""
    file_path = f"backups/{table_name}.avro"

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"AVRO file for {table_name} not found.")

    with open(file_path, 'rb') as f:
        reader = fastavro.reader(f)
        return list(reader)
    

def upload_file_to_blob(local_path: str, blob_name: str):
    """Upload local AVRO file to blob storage."""
    with open(local_path, "rb") as data:
        container_client.upload_blob(name=blob_name, data=data, overwrite=True)


def upload_log_files_to_blob():
    logs = get_log_paths()
    for name, path in logs.items():
        blob_name = os.path.basename(path)
        upload_file_to_blob(path, blob_name)

def download_blob_to_file(blob_name: str, local_path: str):
    """Download blob and save to local path."""
    with open(local_path, "wb") as f:
        blob_data = container_client.download_blob(blob_name)
        f.write(blob_data.readall())


def upload_file_to_bloblogs(local_path: str, blob_name: str):
    with open(local_path, "rb") as data:
        container_client2.upload_blob(name=blob_name, data=data, overwrite=True)


def upload_logs_to_blob():
    log_paths = get_log_paths()
    for log_type, path in log_paths.items():
        blob_name = os.path.basename(path)
        upload_file_to_bloblogs(path, blob_name)