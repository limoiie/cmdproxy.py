import subprocess

from autodict import Options
from flexio import FlexBinaryIO

from cmdproxy.celery_app.config import CmdProxyServerConf
from cmdproxy.invoke_middle import DeserializeAndUnpackMiddle, \
    ProxyServerEndInvokeMiddle


class Server:
    def __init__(self, conf: CmdProxyServerConf):
        @DeserializeAndUnpackMiddle(fmt='json', options=Options(with_cls=False))
        @ProxyServerEndInvokeMiddle(conf.cloud_fs.grid_fs())
        def proxy(command, args, stdout, stderr, env, cwd):
            with FlexBinaryIO(stdout, mode='wb+') as out, \
                    FlexBinaryIO(stderr, mode='wb+') as err:
                res = subprocess.run([command, *args], env=env, cwd=cwd,
                                     stdout=out, stderr=err)
            return res.returncode

        self._proxy = proxy
        self._conf = conf

    def run(self, serialized_request: str):
        return self._proxy(serialized_request)

    @staticmethod
    def instance():
        from cmdproxy.celery_app.config import get_server_end_conf

        conf = get_server_end_conf()
        return Server(conf)
