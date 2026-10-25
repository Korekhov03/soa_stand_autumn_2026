FROM python:3.13-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py processing.py storage.py schema.sql ./
USER 65532:65532
EXPOSE 8080
CMD ["python", "app.py"]
