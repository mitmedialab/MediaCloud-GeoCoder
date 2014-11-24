from __future__ import absolute_import

from celery import Celery
from mediameter import settings

app = Celery('mediameter',
             broker=settings.get('queue','url'),
             backend=settings.get('queue','url'),
             include=['mediameter.tasks'])

# Optional configuration, see the application user guide.
app.conf.update(
    CELERY_TASK_RESULT_EXPIRES=3600,
)

if __name__ == '__main__':
    app.start()