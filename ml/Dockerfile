FROM python:3.12-slim

WORKDIR /app

COPY requirements-serve.txt .
RUN pip install --no-cache-dir -r requirements-serve.txt

COPY serve.py .

ENV PYTHONUNBUFFERED=1

# Model is mounted at runtime: ./ml/model → /app/ml/model
# Run: docker compose up (model must exist at ml/model/fraud_pipeline.joblib)
CMD ["uvicorn", "serve:app", "--host", "0.0.0.0", "--port", "8010"]
