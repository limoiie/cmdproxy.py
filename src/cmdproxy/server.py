import subprocess

from autodict import Options
from flexio import FlexBinaryIO

from cmdproxy.celery_app.config import CmdProxyServerConf
from cmdproxy.logging import get_logger
from cmdproxy.middles import DeserializeAndUnpackMiddle, \
    ProxyServerEndInvokeMiddle

logger = get_logger(__name__)


class Server:
    def __init__(self, conf: CmdProxyServerConf):
        self._conf = conf

    def run(self, serialized_request: str):
        @DeserializeAndUnpackMiddle(fmt='json', options=Options(with_cls=False))
        @ProxyServerEndInvokeMiddle(fs=self._conf.cloud.grid_fs())
        def proxy(command, args, stdout, stderr, env, cwd):
            logger.debug(f'Running command `{command}` with: \n'
                         f'  args: {args}\n'
                         f'  stdout: {stdout}\n'
                         f'  stderr: {stderr}\n'
                         f'  env: {env}\n'
                         f'  cwd: {cwd}\n')

            with FlexBinaryIO(stdout, mode='wb+') as out, \
                    FlexBinaryIO(stderr, mode='wb+') as err:
                res = subprocess.run([command, *args], env=env, cwd=cwd,
                                     stdout=out, stderr=err)
            return res.returncode

        return proxy(serialized_request)

    @staticmethod
    def instance():
        from cmdproxy.celery_app.config import get_server_end_conf

        conf = get_server_end_conf()
        return Server(conf)
