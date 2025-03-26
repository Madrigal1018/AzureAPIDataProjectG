# Azure ETL FastAPI Project

A FastAPI-based REST API that performs data ingestion, validation, batch insertions into Azure SQL, AVRO backup, and restore from Azure Blob Storage.

---

## Features

- ✅ RESTful API with FastAPI
- ✅ Batch data upload (1–1000 rows per table)
- ✅ Strict schema validation using Pydantic
- ✅ Backup each table to AVRO format
- ✅ Restore tables from AVRO files
- ✅ Upload backups and logs to Azure Blob Storage
- ✅ Logs for valid and invalid transactions
- ✅ Modular and testable project structure
- ✅ Dockerized for easy deployment

---

## Project Structure
azure-etl-api/ ├── app/ │ ├── main.py # FastAPI app & endpoints │ ├── models.py # Pydantic schemas │ ├── crud.py # Insert logic │ ├── utils.py # Blob helpers, AVRO I/O │ ├── db.py # Azure SQL connection │ ├── logger.py # Log setup (success/failure) │ └── init.py 

├── scripts/ │ ├── backup_all.py # CLI backup │ └── restore_all.py # CLI restore 

├── tests/ │ └── test_crud.py 

├── logs/ # Local logs (auto-generated) 
├── .env.example # Example env file 
├── requirements.txt 
├── Dockerfile 
├── .gitignore 
└── README.md
├── backup.py # Backup & restore

## Run API
uvicorn app.main:app --reload

## Logs and backups are saved in blob Storage
