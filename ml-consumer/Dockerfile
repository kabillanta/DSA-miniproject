FROM python:3.10-slim

WORKDIR /app

# Create models directory
RUN mkdir -p /app/models

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py model.py ./

CMD ["python", "consumer.py"]