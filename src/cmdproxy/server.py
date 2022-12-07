from autodict import Options

from cmdproxy.celery_app.config import CmdProxyServerConf
from cmdproxy.command_tool import CommandTool
from cmdproxy.invoke_middle import DeserializeAndUnpackMiddle, \
    ProxyServerEndInvokeMiddle


class Server:
    def __init__(self, conf: CmdProxyServerConf):
        @DeserializeAndUnpackMiddle(fmt='json', options=Options(with_cls=False))
        @ProxyServerEndInvokeMiddle(conf.cloud_fs.grid_fs())
        def proxy(command, args, stdout, stderr, env, cwd):
            return_code = CommandTool(command)(
                *args,
                stdout=stdout,
                stderr=stderr,
                env=env,
                cwd=cwd)
            # todo: collect command err
            return return_code

        self._proxy = proxy
        self._conf = conf

    def run(self, serialized_request: str):
        return self._proxy(serialized_request)

    @staticmethod
    def instance():
        from cmdproxy.celery_app.config import get_server_end_conf

        conf = get_server_end_conf()
        return Server(conf)
