from celery import Celery
from celery.schedules import crontab

app = Celery(
    'byn.tasks.launch',
    include=[
        'byn.tasks.bcse',
        'byn.tasks.external_rates',
        'byn.tasks.nbrb',
        'byn.tasks.produce_predict',
    ]
)


app.conf.beat_schedule = {
    'update-nbrb': {
        'task': 'byn.tasks.nbrb.update_nbrb_rates_async',
        'schedule': crontab(minute=0, hour=15, day_of_week=[1, 5]),
    },
}


if __name__ == '__main__':
    app.start()
