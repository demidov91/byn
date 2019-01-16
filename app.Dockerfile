FROM python:3.7-alpine

COPY app /app
WORKDIR /app
RUN pip install requirements/app.txt

CMD ["python", "-m", "launch.py"]