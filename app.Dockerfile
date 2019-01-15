FROM python:3.7-alpine

COPY app /app
WORKDIR /app
RUN pip install requirements.txt

CMD ["python", "-m", "launch.py"]