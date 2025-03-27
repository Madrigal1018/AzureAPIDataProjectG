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
AzureAPIDataProjectG/ ├── app/ │ ├── main.py # FastAPI app & endpoints │ ├── models.py # Pydantic schemas │ ├── crud.py # Insert logic │ ├── utils.py # Blob helpers, AVRO I/O │ ├── db.py # Azure SQL connection │ ├── logger.py # Log setup (success/failure) │ └── init.py 

├── queries/  # Analytics 

├── tests/ │ └── test_crud.py 

├── logs/ # Local logs (auto-generated) 
├── .env.example # Example env file 
├── requirements.txt 
├── Dockerfile 
├── .gitignore 
└── README.md
├── backup.py # Backup & restore

## Requirements
fastapi
uvicorn
pydantic
pandas
fastavro
pyodbc
azure-storage-blob
python-dotenv
requests


## Run API
uvicorn app.main:app --reload

## Logs and backups are saved in blob Storage (Azure)

## Run Docker Container (Bash)
1. Build container : docker build -t azureapidataprojectg .
2. Prepare your .env file with the .env.example file
3. Run containter: docker run -p 8000:8000 --env-file .env azureapidataprojectg
4. Go to http://localhost:8000/docs (Swagger UI to test)

### If Port 8000 Is In Use

Use a different host port (e.g. 8001):
```bash
docker run -p 8001:8000 --env-file .env azureapidataprojectg
```
Then visit:
```
http://localhost:8001/docs
```
---

### Stop a Running Container

List containers:
```bash
docker ps
```

Then stop it:
```bash
docker stop <container_id>
```

### Rebuild After Code or Dependency Changes

If you change the code, Dockerfile, or `requirements.txt`, rebuild the image:

```bash
docker build -t azureapidataprojectg .
```

Then re-run the container using the `docker run` command above.


## Credits
Built with ❤️ using:

FastAPI
Azure SDK for Python
AVRO (fastavro)
Pandas
PyODBC

Mateo Madrigal, Data Engineer