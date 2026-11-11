FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 OMP_NUM_THREADS=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src src
COPY data/demo.csv data/demo.csv
COPY tests tests
CMD ["python", "-m", "src.scorer"]
