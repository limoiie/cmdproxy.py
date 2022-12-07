from celery.utils.log import get_task_logger

from cmdproxy.celery_app.app import app

logger = get_task_logger('cmd-proxy')


@app.task
def run(serialized_request: str):
    from cmdproxy.server import Server

    serialized_response = Server.instance().run(serialized_request)
    return serialized_response
