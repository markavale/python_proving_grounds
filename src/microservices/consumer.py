import asyncio
import requests
from app.services.job_service import JobService
from decouple import config

job_service = JobService()

async def process_job(job_data):
    # Simulate job processing
    await asyncio.sleep(5)
    result = {"status": "success", "data": job_data["data"]}
    return result

async def main():
    while True:
        job_id = await job_service.redis_handler.pop_job_from_queue()
        if job_id:
            job_data = await job_service.redis_handler.get_job(job_id)
            job_data["status"] = "processing"
            await job_service.redis_handler.set_job_status(job_id, job_data)
            
            result = await process_job(job_data)
            
            job_data["status"] = "done"
            job_data["result"] = result
            await job_service.redis_handler.set_job_status(job_id, job_data)
            
            # Update job status in MongoDB
            await job_service.mongo_handler.update_job(job_id, job_data)
            
            if job_data.get("callback_url"):
                requests.post(job_data["callback_url"], json=result)
        else:
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())