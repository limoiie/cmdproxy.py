from celery import Celery

from cmdproxy.celery_app.config import get_celery_conf

conf = get_celery_conf()

app = Celery('tasks', broker=conf.redis_uri, backend=conf.mongo_uri,
             include=['cmdproxy.celery.tasks'])
