from celery import Celery

from cmdproxy.celery_app.config import get_celery_conf

conf = get_celery_conf()

app = Celery('tasks', broker=conf.broker_url, backend=conf.backend_url,
             include=['cmdproxy.celery_app.tasks'])
