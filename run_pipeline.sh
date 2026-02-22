# Exit if any command fails.
set -e

# Flags to control which parts of the pipeline to run.
RUN_SETUP=true
RUN_BATCH=true
RUN_STREAM=false

# Absolute path to the root directory of the project.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Initialize the cluster.
if [ "$RUN_SETUP" = true ] ; then
    echo
    cd "$ROOT_DIR/setup"
    echo "> Building infrastructure..."
    ./cluster_up.sh
    sleep 3
fi

# Run batch processing jobs if specified.
if [ "$RUN_BATCH" = true ] ; then
    cd "$ROOT_DIR/batch_processing/run"
    echo "> Running batch processing jobs..."
    ./batch_jobs_run.sh
    sleep 3
fi

# Run stream processing jobs if specified.
if [ "$RUN_STREAM" = true ] ; then

    # Start the Kafka producer in detached mode.
    cd "$ROOT_DIR/stream_processing"
    echo "> Starting Kafka producer (detached)..."
    # Before starting, kill any existing producer process and redirect logs to the main container log.
    docker-compose exec -d kafka_producer sh -c "pkill -f producer.py || true; python -u producer.py >> /proc/1/fd/1 2>&1"
    echo "Successfully started Kafka producer."
    sleep 3

    # Run the stream processing jobs.
    cd "$ROOT_DIR/stream_processing/run"
    echo
    echo "> Running stream processing jobs..."
    ./stream_jobs_run.sh
    sleep 3
fi
