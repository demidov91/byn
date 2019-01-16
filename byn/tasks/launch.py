from celery import Celery

app = Celery(
    'byn.tasks.launch',
    include=[
        'byn.tasks.bcse',
        'byn.tasks.external_rates',
        'byn.tasks.nbrb',
        'byn.tasks.produce_predict',
    ]
)


if __name__ == '__main__':
    app.start()
