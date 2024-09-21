from pymongo import MongoClient
import redis
import json

class DatabaseHandler:
    def __init__(self, mongo_uri="mongodb://localhost:27017/", redis_host='localhost', redis_port=6379, redis_db=0):
        # MongoDB client
        self.mongo_client = MongoClient(mongo_uri)
        self.db = self.mongo_client["job_db"]
        self.jobs_collection = self.db["jobs"]
        
        # Redis client
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

    def save_job_to_mongo(self, job_data):
        self.jobs_collection.insert_one(job_data)

    def update_job_in_mongo(self, job_id, job_data):
        self.jobs_collection.update_one({"job_id": job_id}, {"$set": job_data})

    def save_job_to_redis(self, job_id, job_data):
        self.redis_client.set(job_id, json.dumps(job_data))
        self.redis_client.lpush("job_queue", job_id)

    def get_job_from_redis(self, job_id):
        job_data = self.redis_client.get(job_id)
        if job_data:
            return json.loads(job_data)
        return None

    def pop_job_from_queue(self):
        job_id = self.redis_client.rpop("job_queue")
        if job_id:
            return job_id.decode("utf-8")
        return None

    def set_job_status_in_redis(self, job_id, job_data):
        self.redis_client.set(job_id, json.dumps(job_data))