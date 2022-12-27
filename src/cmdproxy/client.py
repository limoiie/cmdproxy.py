from typing import cast

import celery
from autodict import Options

from cmdproxy.celery_app.config import CmdProxyClientConf
from cmdproxy.invoke_params import CmdNameParam, CmdParamBase, CmdPathParam
from cmdproxy.middles import PackAndSerializeMiddle, \
    ProxyClientEndInvokeMiddle


class Client:
    def __init__(self, conf: CmdProxyClientConf, run: celery.Task):
        self._conf = conf
        self._run = run

    def run(self, command: CmdParamBase, args, stdout=None, stderr=None,
            env=None, cwd=None, queue=None):
        assert isinstance(command, CmdParamBase), \
            f'Expect command in type of {CmdNameParam} or {CmdPathParam}, ' \
            f'got {type(command)}'

        if isinstance(command, CmdNameParam):
            queue = queue or command.name
        if isinstance(command, CmdPathParam):
            assert queue is not None, \
                f'Queue should be specified when command is instance of ' \
                f'{CmdPathParam}'

        conf = ProxyClientEndInvokeMiddle.Config(cloud=self._conf.cloud)

        @ProxyClientEndInvokeMiddle(conf=conf)
        @PackAndSerializeMiddle(fmt='json', options=Options(with_cls=False))
        def proxy(serialized: str) -> str:
            return self._run.apply_async(args=(serialized,), queue=queue).get()

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
