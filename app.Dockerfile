FROM python:3.7-alpine

ENV TZ=Europe/Minsk
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY app /app
WORKDIR /app
RUN pip install requirements/app.txt

CMD ["python", "-m", "launch.py"]