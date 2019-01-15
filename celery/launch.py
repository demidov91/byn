from celery import Celery

app = Celery(
    include=[
        'bcse',
        'external_rates',
        'nbrb',
        'produce_predict',
    ]
)


if __name__ == '__main__':
    app.start()
