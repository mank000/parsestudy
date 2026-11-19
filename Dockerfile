FROM python:3.14-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY catalog ./catalog
USER 10001:10001
CMD ["sh", "-ec", "python -m catalog init; exec python -m catalog.dashboard --host 0.0.0.0"]
