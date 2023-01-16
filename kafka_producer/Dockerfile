FROM python:3.11
WORKDIR /app

RUN python -m venv venv && . venv/bin/activate
ENV PYTHONPATH "${PYTHONPATH}:/app"

COPY requirements.txt .
RUN python -m pip install --upgrade pip && \
    pip install -r requirements.txt --no-cache-dir --ignore-installed

COPY src .

EXPOSE 8000

RUN gunicorn --worker-class gevent --preload\
    --workers $WORKERS \
    --bind 0.0.0.0:$PORT_APP \
    --log-level $LOGGING_LEVEL \
    wsgi_app:app