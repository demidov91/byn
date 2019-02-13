from celery import Celery
from celery.schedules import crontab

# Initialize logging configurations.
import byn.logging

app = Celery(
    'byn.tasks.launch',
    include=[
        'byn.tasks.backup',
        'byn.tasks.external_rates',
        'byn.tasks.nbrb',
        'byn.realtime.external_rates',
    ]
)

app.conf['task_serializer'] = app.conf['result_serializer'] = 'pickle'
app.conf['accept_content'] = ['pickle']
app.conf['enable_utc'] = False

app.conf.beat_schedule = {
    'update-nbrb': {
        'task': 'byn.tasks.nbrb.update_nbrb_rates_async',
        'schedule': crontab(minute=0, hour=14, day_of_week='mon-fri'),
    },
    'backup': {
        'task': 'byn.tasks.backup.backup_async',
        'schedule': crontab(minute=30, hour=1),
    },
}


if __name__ == '__main__':
    app.start()
