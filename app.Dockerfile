FROM python:3.7-alpine

ENV TZ=Europe/Minsk
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY byn /byn
RUN pip install /byn/requirements/app.txt

CMD ["python", "-m", "launch.py"]