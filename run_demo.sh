#!/usr/bin/env bash
set -euo pipefail

# This function will be called when the script exits, ensuring a clean shutdown.
function cleanup() {
  echo "" # Newline for cleaner output
  echo "Shutting down Docker containers..."
  # Use --volumes to also remove the anonymous volumes created by Kafka
  docker compose down --volumes
}

# 'trap' sets up a command to run when the script receives a signal.
# We trap EXIT, INT (Ctrl+C), and TERM signals to run our cleanup function.
trap cleanup EXIT INT TERM

# Set up paths
ROOT_DIR="$(pwd)"
FLINK_DIR="$ROOT_DIR/flink"
PYFILES_ZIP="$ROOT_DIR/flink/flink_job_deps.zip"
JOB_SCRIPT="/job-src/flink/ctr.py"

cd "$ROOT_DIR"

rm -rf output
mkdir -p output

# 1. Start the docker-compose cluster in the background
echo "Starting Flink + Kafka cluster with 'docker compose up -d'..."
docker compose up -d

# Give Kafka a moment to be ready before creating topics
echo "Waiting for Kafka to be ready..."
sleep 5

# 2. Create Kafka topics
echo "Creating Kafka topics: 'impressions' and 'clicks'..."
docker exec -it kafka bash -lc '
kafka-topics --bootstrap-server kafka:29092 --create --topic impressions --partitions 3 --replication-factor 1 --if-not-exists
kafka-topics --bootstrap-server kafka:29092 --create --topic clicks --partitions 3 --replication-factor 1 --if-not-exists
'

echo "Submitting Flink CTR job to the cluster..."
docker exec jobmanager /opt/flink/bin/flink run -d -py "$JOB_SCRIPT" --pyFiles /job-src/flink/flink_job_deps.zip &

echo ""
echo "✅ Flink job submitted successfully!"
echo "➡️ Flink UI: http://localhost:8081"
echo "➡️ Confluent Control Center: http://localhost:9021"
echo ""

# 3. Start the Go data producer in the foreground
echo "🚀 Starting data producer. Press [Ctrl+C] to stop."
echo "----------------------------------------------------"
go run ./producer/main.go

# The 'trap' will automatically call the 'cleanup' function when you press Ctrl+C