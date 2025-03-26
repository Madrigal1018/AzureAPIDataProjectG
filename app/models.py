from pydantic import BaseModel, Field
from typing import List
from datetime import date
from typing import Optional

class HiredEmployee(BaseModel):
    id: int
    name: Optional[str] = None
    datetime: Optional[str] = None
    department_id: Optional[int] = None
    job_id: Optional[int] = None

class Department(BaseModel):
    id: int
    department: str

class Job(BaseModel):
    id: int
    job: str

class BatchData(BaseModel):
    hired_employees: List[HiredEmployee]
    departments: List[Department]
    jobs: List[Job]