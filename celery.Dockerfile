FROM python:3.7-alpine

COPY app /app
WORKDIR /app
RUN pip install requirements/celery.txt

CMD flower & celery -A launch worker
