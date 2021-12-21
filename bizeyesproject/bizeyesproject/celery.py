from __future__ import absolute_import

import os
from celery._state import _set_current_app

from celery import Celery

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'bizeyesproject.settings')
BASE_REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')
#the first argument is the name of the current module
app = Celery('bizeyesproject', backend='redis://localhost', broker='pyamqp://')
_set_current_app(app)
# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

@app.task(bind=True)
def debug_task(self):
    print('Request: {0!r}'.format(self.request))
    #print(f'Request: {self.request!r}')