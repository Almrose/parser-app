
from __future__ import absolute_import, unicode_literals
import os
from config.settings import base
from datetime import timedelta


import os
from celery import Celery


app = Celery("resume_daily")

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object(base)

app.autodiscover_tasks()

app.conf.beat_schedule = {
    'resume_list_parse': {
        'task': 'Resume Cards Parsing',
        'schedule': timedelta(seconds=30)  # execute
    },
    'parsing_unparsed_resumes': {
        'task': 'Unparsed Resume Parsing',
        'schedule': timedelta(seconds=40)
    },
    'tocken_check': {
        'task': 'Token Check',
        'schedule': timedelta(seconds=60)
    },
    'upload_to_db': {
        'task': 'Upload To Remote Table',
        'schedule': timedelta(seconds=60)
    },
    # 'print_test': {
    #     'task': "worker.tasks.print_test",
    #     'schedule': timedelta(seconds=5)
    # }
}
