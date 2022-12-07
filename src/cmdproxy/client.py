from typing import cast

import celery
from autodict import Options

from cmdproxy.celery_app.config import CmdProxyClientConf, init_client_end_conf
from cmdproxy.invoke_middle import PackAndSerializeMiddle, \
    ProxyClientEndInvokeMiddle


class Client:
    def __init__(self, conf: CmdProxyClientConf, run: celery.Task):
        # todo: resolve config or environment vars?
        @ProxyClientEndInvokeMiddle(conf.celery.grid_fs())
        @PackAndSerializeMiddle(fmt='json', options=Options(with_cls=False))
        def proxy(serialized: str):
            # all the args has been converted into strings
            return_code = run.delay(serialized).get()
            # todo: collect command err
            return return_code

        self._proxy = proxy
        self._conf = conf

    def run(self, command, args, stdout=None, stderr=None, env=None, cwd=None):
        return self._proxy(command=command,
                           args=args,
                           stdout=stdout,
                           stderr=stderr,
                           env=env,
                           cwd=cwd)


def startup_app(redis_uri, mongo_uri, mongodb_name='cmdproxy'):
    from cmdproxy.celery_app.tasks import run

    conf = init_client_end_conf(redis_uri, mongo_uri, mongodb_name)

    return Client(conf, cast(celery.Task, run))
