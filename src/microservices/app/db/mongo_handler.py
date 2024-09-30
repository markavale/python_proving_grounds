from motor.motor_asyncio import AsyncIOMotorClient
from decouple import Config

config = Config()

class MongoHandler:
    def __init__(self, mongo_uri=None):
        mongo_uri = mongo_uri or config("MONGO_URI")
        self.mongo_client = AsyncIOMotorClient(mongo_uri)
        self.db = self.mongo_client["job_db"]
        self.jobs_collection = self.db["jobs"]

    async def save_job(self, job_data):
        await self.jobs_collection.insert_one(job_data)

    async def update_job(self, job_id, job_data):
        await self.jobs_collection.update_one({"job_id": job_id}, {"$set": job_data})