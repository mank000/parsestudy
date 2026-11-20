FROM python:3.14-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV HOME=/home/catalog
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY catalog ./catalog
RUN install -d -m 0750 -o 10001 -g 10001 "$HOME"
USER 10001:10001
CMD ["sh", "-ec", "python -m catalog init; exec python -m catalog.dashboard --host 0.0.0.0"]
