from __future__ import absolute_import

from celery import Celery
from mediameter import settings

app = Celery('mediameter',
             broker=settings.get('queue','broker_url'),
             backend=settings.get('queue','backend_url'),
             include=['mediameter.tasks'])

# expire backend results in one hour
app.conf.update(
    CELERY_TASK_RESULT_EXPIRES=3600,
)

if __name__ == '__main__':
    app.start()