FROM python:3.10-slim

WORKDIR /app

# Instalar dependências do sistema se necessário
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# Melhor prática: atualizar pip
RUN pip install --upgrade pip

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

# Expor a porta
EXPOSE 1433

CMD ["python", "webhook_server.py"]
