FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 1433

CMD ["python", "webhook_server.py"]
