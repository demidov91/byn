FROM python:3.7-alpine

ENV TZ=Europe/Minsk
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY byn /byn
RUN pip install /byn/requirements/celery.txt

CMD flower & celery -A launch worker
