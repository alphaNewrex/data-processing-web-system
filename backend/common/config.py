import os


class Settings:
    RABBITMQ_URL: str = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672//")
    MONGO_URL: str = os.getenv("MONGO_URL", "mongodb://localhost:27017")
    MONGO_DB: str = os.getenv("MONGO_DB", "data_processing")
    MONGO_COLLECTION: str = "datasets"

    # Object storage (MinIO / S3)
    S3_ENDPOINT: str = os.getenv("S3_ENDPOINT", "http://localhost:9000")
    S3_BUCKET: str = os.getenv("S3_BUCKET", "datasets")
    S3_ACCESS_KEY: str = os.getenv("S3_ACCESS_KEY", "minioadmin")
    S3_SECRET_KEY: str = os.getenv("S3_SECRET_KEY", "minioadmin")
    S3_REGION: str = os.getenv("S3_REGION", "us-east-1")

    # Pipeline behaviour
    # Artificial per-stage delay to make progress visible in demos. Default 0 in prod/tests.
    SIMULATE_DELAY_SECONDS: int = int(os.getenv("SIMULATE_DELAY_SECONDS", "0"))


settings = Settings()
