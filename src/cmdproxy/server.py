import pathlib

from autodict import Options

from cmdproxy.celery_app.config import CmdProxyServerConf, init_server_end_conf
from cmdproxy.command_tool import CommandTool
from cmdproxy.invoke_middle import DeserializeAndUnpackMiddle, \
    ProxyServerEndInvokeMiddle
from cmdproxy.singleton import Singleton


class Server(Singleton):
    def __init__(self, conf: CmdProxyServerConf):
        # todo: resolve config or environment vars?
        @DeserializeAndUnpackMiddle(fmt='json', options=Options(with_cls=False))
        @ProxyServerEndInvokeMiddle(conf.celery.grid_fs())
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


def startup_app(redis_uri: str, mongo_uri: str, mongodb_name: str,
                command_palette_path: str):
    conf = init_server_end_conf(redis_uri, mongo_uri, mongodb_name,
                                pathlib.Path(command_palette_path))

    return Server.instantiate(conf)
