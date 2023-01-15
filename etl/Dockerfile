FROM python:3.11-alpine

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint
RUN chmod +x /usr/local/bin/docker-entrypoint

WORKDIR /srv/app

COPY requirements.txt /srv/app/
RUN pip install -r requirements.txt

COPY main.py /srv/app/
COPY etl /srv/app/etl

ENTRYPOINT ["docker-entrypoint"]
CMD ["/usr/local/bin/python", "/srv/app/main.py"]
