#!/bin/bash
# Local development runner (without Docker)
# Prerequisites: RabbitMQ and MongoDB running locally

set -e

cd "$(dirname "$0")"

# Start Celery worker in background
echo "Starting Celery worker..."
cd backend
PYTHONPATH=. celery -A workers.celery_app worker --loglevel=info --concurrency=2 &
CELERY_PID=$!

# Start FastAPI server
echo "Starting FastAPI server..."
PYTHONPATH=. uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload &
API_PID=$!

# Trap to kill both on exit
trap "kill $CELERY_PID $API_PID 2>/dev/null; exit" SIGINT SIGTERM

echo ""
echo "API running at http://localhost:8000"
echo "API docs at http://localhost:8000/docs"
echo "Press Ctrl+C to stop"

wait
