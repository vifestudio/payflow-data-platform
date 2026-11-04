FROM python:3.10-slim
WORKDIR /airbyte/integration_code
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY source.py main.py spec.yaml ./
COPY schemas/ ./schemas/
ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
