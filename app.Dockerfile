FROM python:3.7-stretch

ENV TZ=Europe/Minsk
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY byn /byn
RUN mkdir -p /data/ridge_cache

# It looks like a bug in a thrift dependencies. It is not installable without cython.
RUN pip install cython
RUN pip install -r /byn/requirements/app.txt

CMD ["python", "-m", "byn.realtime.launch"]
