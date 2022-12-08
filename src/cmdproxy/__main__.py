from typing import Optional

import fire

from cmdproxy.celery_app.config import init_server_conf


def launch(conf_path: Optional[str] = None, redis_url: Optional[str] = None,
           mongo_url: Optional[str] = None, mongodb_name: Optional[str] = None,
           command_palette: Optional[str] = None):
    init_server_conf(conf_path=conf_path, redis_url=redis_url,
                     mongo_url=mongo_url, mongodb_name=mongodb_name,
                     command_palette=command_palette)

    from cmdproxy.celery_app.app import app
    app.start()


if __name__ == '__main__':
    fire.Fire(launch)
