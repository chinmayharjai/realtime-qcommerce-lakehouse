FROM python:3.12-slim

WORKDIR /app

# confluent-kafka wheels are prebuilt, so no compiler is needed — the slim image
# is enough. Pinned to match requirements.txt so the container and a local run use
# the same client.
RUN pip install --no-cache-dir confluent-kafka==2.6.1

COPY order_stream.py .

# ENTRYPOINT, not CMD, so `docker compose run --rm producer --duration 120`
# appends flags to the script rather than replacing the whole command.
ENTRYPOINT ["python", "order_stream.py"]
