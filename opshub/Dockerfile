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

# Flask app listens on 8080 (matches the Google OAuth redirect URI already
# registered as http://localhost:8080/auth/callback)
EXPOSE 8080

# Runs as a non-root user for basic container hygiene. her_revocation.py writes
# two files next to itself at runtime — .her_revoke_token.json (today's shared
# revoke token) and her_revocation_audit.db (the revocation audit log) — so
# appuser needs write access to /app, not just read, or both will fail with a
# PermissionError the first time anyone uses the HER Revocation tab.
RUN useradd --create-home appuser && chown -R appuser:appuser /app
USER appuser

CMD ["python3", "server.py"]
