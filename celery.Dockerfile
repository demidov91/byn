FROM dzmitry/byn-app

RUN pip install -r /byn/requirements/celery.txt

CMD flower --basic_auth=$FLOWER_BASIC_AUTH & celery -A byn.tasks.launch -c 2 worker -B
