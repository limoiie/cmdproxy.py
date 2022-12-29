import subprocess

import flexio
from autodict import Options

from cmdproxy.celery_app.config import CmdProxyServerConf
from cmdproxy.logging import get_logger
from cmdproxy.middles import DeserializeAndUnpackMiddle, \
    ProxyServerEndInvokeMiddle

logger = get_logger(__name__)


class Server:
    def __init__(self, conf: CmdProxyServerConf):
        self._conf = conf

    def run(self, serialized_request: str):
        conf = ProxyServerEndInvokeMiddle.Config(
            cloud=self._conf.cloud, command_palette=self._conf.command_palette)

        @DeserializeAndUnpackMiddle(fmt='json', options=Options(with_cls=False))
        @ProxyServerEndInvokeMiddle(conf=conf)
        def proxy(command, args, stdout, stderr, env, cwd):
            logger.debug(f'Running command `{command}` with: \n'
                         f'  args: {args}\n'
                         f'  stdout: {stdout}\n'
                         f'  stderr: {stderr}\n'
                         f'  env: {env}\n'
                         f'  cwd: {cwd}\n')

            with flexio.flex_open(stdout, mode='w+b') as stdout, \
                    flexio.flex_open(stderr, mode='w+b') as stderr:
                res = subprocess.run([command, *args], env=env, cwd=cwd,
                                     stdout=stdout, stderr=stderr)
            return res.returncode

        return proxy(serialized_request)

    @staticmethod
    def instance():
        from cmdproxy.celery_app.config import get_server_end_conf

        conf = get_server_end_conf()
        return Server(conf)
