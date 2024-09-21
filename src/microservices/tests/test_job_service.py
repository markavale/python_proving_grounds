import pytest
import asynctest
from app.services.job_service import JobService
from app.db.mongo_handler import MongoHandler
from app.db.redis_handler import RedisHandler

class TestJobService(asynctest.TestCase):
    async def setUp(self):
        self.mongo_handler = asynctest.Mock(spec=MongoHandler)
        self.redis_handler = asynctest.Mock(spec=RedisHandler)
        self.job_service = JobService()
        self.job_service.mongo_handler = self.mongo_handler
        self.job_service.redis_handler = self.redis_handler

    async def test_create_job(self):
        job_id = "test_job_id"
        job_data = {"job_id": job_id, "data": {}, "status": "pending", "callback_url": None}

        await self.job_service.create_job(job_id, job_data)

        self.mongo_handler.save_job.assert_awaited_once_with(job_data)
        self.redis_handler.save_job.assert_awaited_once_with(job_id, job_data)

    async def test_get_job_status(self):
        job_id = "test_job_id"
        job_data = {"job_id": job_id, "data": {}, "status": "pending", "callback_url": None}

        self.redis_handler.get_job.return_value = job_data

        result = await self.job_service.get_job_status(job_id)

        self.redis_handler.get_job.assert_awaited_once_with(job_id)
        assert result == job_data

if __name__ == "__main__":
    pytest.main()