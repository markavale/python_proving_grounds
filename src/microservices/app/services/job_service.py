from ..db.mongo_handler import MongoHandler
from ..db.redis_handler import RedisHandler

class JobService:
    def __init__(self):
        self.mongo_handler = MongoHandler()
        self.redis_handler = RedisHandler()

    async def create_job(self, job_id, job_data):
        await self.mongo_handler.save_job(job_data)
        await self.redis_handler.save_job(job_id, job_data)

    async def get_job_status(self, job_id):
        return await self.redis_handler.get_job(job_id)