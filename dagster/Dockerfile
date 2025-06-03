FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY marketcrawler .
COPY dagster.yaml /app/dagster_storage/
EXPOSE 3000
CMD ["dagster", "dev", "-m", "marketcrawler", "--host", "0.0.0.0"]