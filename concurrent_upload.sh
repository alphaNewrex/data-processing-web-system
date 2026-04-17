#!/bin/bash
# Upload multiple datasets concurrently to test concurrent processing.
# Usage: bash concurrent_upload.sh

API="http://localhost:8000/api/dataset"
DIR="$(dirname "$0")/sample_data"

echo "=== Concurrent Upload Test ==="
echo "Uploading all sample datasets simultaneously..."
echo ""

for f in "$DIR"/*.json; do
  filename=$(basename "$f")
  echo "Uploading $filename..."
  curl -s -X POST "$API" -F "file=@$f" &
done

# Wait for all uploads to finish
wait

echo ""
echo ""
echo "=== All uploads submitted ==="
echo "Monitor progress at http://localhost:5173 or:"
echo "  curl -s http://localhost:8000/api/datasets | python3 -m json.tool"
