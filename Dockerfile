FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    bash \
    default-mysql-client \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /gametracker

# Copier les dépendances d'abord (meilleur cache Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le reste du projet
COPY . .

RUN chmod +x scripts/*.sh || true

CMD ["bash"]

