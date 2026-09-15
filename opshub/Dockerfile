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

# Flask app listens on 8080 (matches the Google OAuth redirect URI already
# registered as http://localhost:8080/auth/callback)
EXPOSE 8080

# Runs as a non-root user for basic container hygiene
RUN useradd --create-home appuser
USER appuser

CMD ["python3", "server.py"]
