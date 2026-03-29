FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline/data_pipeline.py .

RUN mkdir -p logs

CMD ["python", "-u", "data_pipeline.py"]
