FROM python:3.10-slim



# Systemabhängigkeiten installieren
RUN apt-get update && apt-get install -y git && apt-get clean\
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Arbeitsverzeichnis
WORKDIR /app

# Anforderungen installieren
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# App-Code kopieren
COPY . .

# Standard-Command (kann durch docker-compose überschrieben werden)
CMD ["python", "playground.py"]
