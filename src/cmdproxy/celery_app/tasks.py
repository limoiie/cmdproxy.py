from celery.utils.log import get_task_logger

from cmdproxy.celery_app.app import app

logger = get_task_logger('cmd-proxy')


@app.task
def run(serialized_request: str):
    import traceback
    from cmdproxy import server

    logger.info(f'Received request: {serialized_request}')

    try:
        ret_code = server.Server.instance().run(serialized_request)
        # todo: handle response = RunCompleteResponse(ret_code, err=tool.err())
        return ret_code
    except KeyError as e:
        ret_code = -9
        err = traceback.format_exc()
        exc = e
    except Exception as e:
        ret_code = -1
        err = traceback.format_exc()
        exc = e

    logger.warning(f'Failed to run the command: {exc}\n  {err}')
    # todo: handle response = RunCompleteResponse(return_code=err.code, err=err)
    return ret_code
