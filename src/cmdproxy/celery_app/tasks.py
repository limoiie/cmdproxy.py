from cmdproxy.celery_app.app import app
from cmdproxy.logging import get_logger

logger = get_logger(__name__)


@app.task(name='run')
def run(serialized_request: str):
    logger.debug(f'Received request: {serialized_request}')

    from cmdproxy.server import Server

    serialized_response = Server.instance().run(serialized_request)
    return serialized_response
