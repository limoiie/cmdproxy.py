import fire

from cmdproxy.celery_app.config import init_server_conf


def launch(*, conf_path: str = None, redis_url: str = None,
           mongo_url: str = None, mongo_dbname: str = None,
           command_palette: str = None, environments: str = None,
           queues: str = None, concurrency: int = None,
           hostname: str = None, detach: str = None, time_limit: float = None,
           pool: str = None, events: bool = None, loglevel: str = None):
    """
    Run the cmdproxy server.

    :param conf_path: Path to the configuration file, default as
      "~/.cmdproxy/server.yaml".
    :param redis_url: Url to the redis, which is for celery broker. When not
      specified, read it from env var named `CMDPROXY_REDIS_URL`, or read from
      configuration file, or default as "redis://localhost:6379".
    :param mongo_url: Url to the mongodb, which is for celery backend and cloud.
      When not specified, read it from env var named `CMDPROXY_MONGO_URL`, or
      read from configuration file, or default as "mongodb://localhost:27017".
    :param mongo_dbname: Database name of cloud fs. When not specified, read it
      from env var named `CMDPROXY_MONGO_DBNAME`, or read from configuration
      file, or default as "cmdproxy".
    :param command_palette: Path to command palette file. When not specified,
      read it from env var named `CMDPROXY_COMMAND_PALETTE`, or read from
      configuration file, or default as `None`.
    :param environments: Path to an environment file.
    :param queues: A list of consume extended queues, separated by comma.
    :param concurrency: The number of working processes.
    :param hostname: Set custom hostname (e.g., 'w1@%%h').
      Expands: %%h (hostname), %%n (name) and %%d (domain).
    :param detach: Start worker as a background process.
    :param time_limit: Enables a hard time limit (in seconds) for tasks.
    :param pool: Pool implementation, which can be any of ['prefork',
      'eventlet', 'gevent', 'solo', 'processes', 'threads'].
    :param events: Send task-related events that can be captured by monitors
      like celery events, celerymon, and others.
    :param loglevel: The log level, can be any one of ['DEBUG', 'INFO',
      'WARNING', 'ERROR', 'FATAL', 'CRITICAL'].
    """
    conf = init_server_conf(conf_path=conf_path, redis_url=redis_url,
                            mongo_url=mongo_url, mongo_dbname=mongo_dbname,
                            command_palette=command_palette,
                            environments=environments, loglevel=loglevel,
                            queues=queues)

    # noinspection PyProtectedMember
    from celery import maybe_patch_concurrency

    if pool in ('eventlet', 'qevent'):
        maybe_patch_concurrency()

    argv = ['worker']
    argv += [f'--queues', conf.celery.queues]
    argv += [f'--concurrency', concurrency] if concurrency else []
    argv += [f'--loglevel', conf.logging.loglevel]
    argv += [f'--hostname', hostname] if hostname else []
    argv += [f'--detach'] if detach else []
    argv += [f'--time-limit', time_limit] if time_limit else []
    argv += [f'--pool', pool] if pool else []
    argv += [f'--events'] if events else []

    from cmdproxy.celery_app.app import app
    app.start(argv)


if __name__ == '__main__':
    fire.Fire(launch)
