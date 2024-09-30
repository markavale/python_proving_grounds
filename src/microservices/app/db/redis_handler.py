import aioredis
import json
from decouple import Config

config = Config()

class RedisHandler:
    def __init__(self, redis_uri=None):
        redis_uri = redis_uri or config("REDIS_URI")
        self.redis_client = aioredis.from_url(redis_uri)

    async def save_job(self, job_id, job_data):
        await self.redis_client.set(job_id, json.dumps(job_data))
        await self.redis_client.lpush("job_queue", job_id)

    async def get_job(self, job_id):
        job_data = await self.redis_client.get(job_id)
        if job_data:
            return json.loads(job_data)
        return None

    async def pop_job_from_queue(self):
        job_id = await self.redis_client.rpop("job_queue")
        if job_id:
            return job_id.decode("utf-8")
        return None

    async def set_job_status(self, job_id, job_data):
        await self.redis_client.set(job_id, json.dumps(job_data))