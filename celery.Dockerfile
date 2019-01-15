FROM python:3.7-alpine

COPY celery /celery
WORKDIR /celery
RUN pip install requirements.txt

CMD flower & celery -A launch worker
