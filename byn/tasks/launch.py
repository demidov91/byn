from celery import Celery
from celery.schedules import crontab

app = Celery(
    'byn.tasks.launch',
    include=[
        'byn.tasks.bcse',
        'byn.tasks.backup',
        'byn.tasks.external_rates',
        'byn.tasks.nbrb',
        'byn.tasks.produce_predict',
    ]
)

app.conf['task_serializer'] = app.conf['result_serializer'] = 'pickle'
app.conf['accept_content'] = ['pickle']
app.conf['enable_utc'] = False

app.conf.beat_schedule = {
    'update-nbrb': {
        'task': 'byn.tasks.nbrb.update_nbrb_rates_async',
        'schedule': crontab(minute=0, hour=15, day_of_week=[1, 5]),
    },
    'update-external-rates': {
        'task': 'byn.tasks.external_rates.update_all_currencies_async',
        'schedule': crontab(minute=0, hour=1),
    },
    'backup': {
        'task': 'byn.tasks.backup.backup_async',
        'schedule': crontab(minute=30, hour=1),
    },
}


if __name__ == '__main__':
    app.start()
