from typing import cast

import celery
from autodict import Options

from cmdproxy.celery_app.config import CmdProxyClientConf
from cmdproxy.invoke_middle import PackAndSerializeMiddle, \
    ProxyClientEndInvokeMiddle


class Client:
    def __init__(self, conf: CmdProxyClientConf, run: celery.Task):
        @ProxyClientEndInvokeMiddle(conf.cloud.grid_fs())
        @PackAndSerializeMiddle(fmt='json', options=Options(with_cls=False))
        def proxy(serialized: str) -> str:
            return run.delay(serialized).get()

        self._proxy = proxy
        self._conf = conf

    def run(self, command, args, stdout=None, stderr=None, env=None, cwd=None):
        return self._proxy(command=command,
                           args=args,
                           stdout=stdout,
                           stderr=stderr,
                           env=env,
                           cwd=cwd)

    @staticmethod
    def instance():
        from cmdproxy.celery_app.config import get_client_end_conf
        from cmdproxy.celery_app.tasks import run

        conf = get_client_end_conf()
        return Client(conf, cast(celery.Task, run))
