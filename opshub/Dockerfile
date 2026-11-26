# OpsHub — PS Internal Ops Dashboard
# Build:  docker build -t opshub .
# Run:    docker run -p 8080:8080 --env-file .env -v ~/.aws:/root/.aws:ro opshub

FROM python:3.11-slim

WORKDIR /app

# Install dependencies first so this layer is cached and doesn't
# reinstall on every code change — only rebuilds when requirements.txt changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY server.py .
COPY index.html .
# her_revocation.py backs the HER Revocation tab — server.py imports it directly,
# so the app won't even start without this file present in the image.
COPY her_revocation.py .
# sqs_monitor.py backs the SQS Queues tab (also imported by server.py at startup);
# queue_boxes.json maps each queue to the EC2 instances that process it.
COPY sqs_monitor.py .
COPY queue_boxes.json .
# starburst_ids.py backs the HER tab's "Fetch from Starburst" lookup — server.py
# imports it at startup, so leaving it out of the image crashes the whole app
# with a ModuleNotFoundError (same failure mode as her_revocation.py above).
COPY starburst_ids.py .
# OpsHub tabs: PS Dashboard (Jira) and Seasonal Email (+ its pilot defaults).
COPY ps_dashboard.py .
COPY seasonal_email.py .
COPY config/ ./config/

# Flask app listens on 8080 (matches the Google OAuth redirect URI already
# registered as http://localhost:8080/auth/callback)
EXPOSE 8080

# Runs as a non-root user for basic container hygiene. her_revocation.py writes
# .her_revoke_token.json (today's shared revoke token) next to itself at runtime
# (the audit log lives in S3 now) — so
# appuser needs write access to /app, not just read, or it will fail with a
# PermissionError the first time anyone saves a token in the HER Revocation tab.
RUN useradd --create-home appuser && chown -R appuser:appuser /app
USER appuser

CMD ["python3", "server.py"]
