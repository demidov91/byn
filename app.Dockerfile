FROM python:3.7-alpine

ENV TZ=Europe/Minsk
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY byn /byn

RUN apk update && apk upgrade && \
    apk add --no-cache bash git musl-dev gcc

RUN pip install -r /byn/requirements/app.txt

CMD ["python", "-m", "launch.py"]