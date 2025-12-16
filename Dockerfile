FROM python:3.12.3-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Use tini for clean signal handling (optional but recommended for long-running loops)

WORKDIR /app

# ---- Python deps (cached layer) ----
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# ---- App code ----
# Assumes your code is at ./post_slovenia/data_ingestor.py
COPY post_slovenia/ /app/post_slovenia/

# If your shipments file lives inside post_slovenia/, copy a build-time snapshot
# (runtime can still bind-mount a fresh file via docker-compose)
# This line will fail if the file doesn't exist; comment it out if you prefer only runtime mount.
COPY post_slovenia/shipments.txt /app/shipments.txt

# ---- Least-privilege user ----
RUN useradd -ms /bin/bash appuser
USER appuser

# No ports exposed (headless worker)
#ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "post_slovenia.data_ingestor"]
