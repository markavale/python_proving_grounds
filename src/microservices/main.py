from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional
import uuid
from app.services.job_service import JobService
from fastapi.encoders import jsonable_encoder
from decouple import config

app = FastAPI()

# Job service
job_service = JobService()

class JobRequest(BaseModel):
    data: dict
    callback_url: Optional[str] = None

class JobStatus(BaseModel):
    job_id: str
    status: str
    result: Optional[dict] = None

@app.post("/jobs", response_model=JobStatus)
async def create_job(request: JobRequest):
    job_id = str(uuid.uuid4())
    job_data = {
        "job_id": job_id,
        "data": request.data,
        "status": "pending",
        "callback_url": request.callback_url
    }
    await job_service.create_job(job_id, job_data)
    return jsonable_encoder(JobStatus(job_id=job_id, status="pending").to_dict())

@app.get("/jobs/{job_id}", response_model=JobStatus)
async def get_job_status(job_id: str):
    job_data = await job_service.get_job_status(job_id)
    if not job_data:
        raise HTTPException(status_code=404, detail="Job not found")
    return jsonable_encoder(JobStatus(job_id=job_id, status=job_data["status"], result=job_data.get("result")))