FROM dzmitry/byn-app

RUN pip install -r /byn/requirements/celery.txt

CMD flower & celery -A launch worker -B
