FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY aq ./aq

RUN useradd --create-home --uid 10001 app \
    && mkdir -p /data/aq && chown -R app:app /data /srv
USER app

ENV AQ_DB_PATH=/data/aq/aq.sqlite3 \
    AQ_INSTANCE_ID=collector

EXPOSE 8000

HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=20 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=2).status==200 else 1)"

CMD ["python", "-m", "aq"]
