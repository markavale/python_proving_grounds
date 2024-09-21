# Saga Pattern Microservice

This project implements a saga pattern microservice architecture using FastAPI, Redis, and MongoDB. The architecture includes an API layer, a message broker, and a consumer layer, all containerized using Docker.

## Architecture

- **API Layer**: FastAPI
  - Handles job creation, status checking, and result retrieval.
  - Saves jobs to MongoDB and Redis.
- **Message Broker**: Redis
  - Queues jobs for processing.
- **Consumer Layer**: Python
  - Processes jobs from the Redis queue.
  - Updates job status in MongoDB and Redis.

## Technology Stack

- FastAPI
- Redis
- MongoDB
- Docker
- Docker Compose

## Setup

### Prerequisites

- Docker
- Docker Compose

### Installation

1. Clone the repository:
   ```sh
   git clone https://github.com/your-repo/saga-pattern-microservice.git
   cd saga-pattern-microservice
   ```

2. Build and run the services using Docker Compose:
   ```sh
   docker-compose up --build
   ```

### API Endpoints

- **Create Job**: `POST /jobs`
- **Get Job Status**: `GET /jobs/{job_id}`

## Project Structure

- `app/`: Contains the FastAPI application and database handlers.
- `consumer/`: Contains the consumer script.
- `Dockerfile`: Docker configuration for the API and consumer.
- `docker-compose.yml`: Docker Compose configuration.

## License

This project is licensed under the MIT License.