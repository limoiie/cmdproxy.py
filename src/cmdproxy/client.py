from typing import cast

import celery
from autodict import Options

from cmdproxy.celery_app.config import CmdProxyClientConf
from cmdproxy.middles import PackAndSerializeMiddle, \
    ProxyClientEndInvokeMiddle


class Client:
    def __init__(self, conf: CmdProxyClientConf, run: celery.Task):
        self._conf = conf
        self._run = run

    def run(self, command, args, stdout=None, stderr=None, env=None, cwd=None,
            queue=None):
        @ProxyClientEndInvokeMiddle(self._conf.cloud.grid_fs())
        @PackAndSerializeMiddle(fmt='json', options=Options(with_cls=False))
        def proxy(serialized: str) -> str:
            return self._run.apply_async(args=(serialized,),
                                         queue=queue or str(command)).get()

        return proxy(command=command,
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
