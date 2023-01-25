FROM python:3.11
WORKDIR /app

RUN python -m venv /venv && . /venv/bin/activate
ENV PYTHONPATH "${PYTHONPATH}:/app"

COPY requirements.txt .
RUN python -m pip install --upgrade pip && \
    pip install -r requirements.txt --no-cache-dir --ignore-installed

COPY src .

EXPOSE 8000

CMD gunicorn --worker-class gevent --preload \
    --workers $API_WORKERS \
    --bind 0.0.0.0:8000 \
    --log-level $API_LOGGING_LEVEL \
    wsgi_app:app