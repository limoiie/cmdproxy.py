import pathlib
from typing import Union

from autodict import Options

from cmdproxy.celery_app.config import CmdProxyServerConf, init_server_conf
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


def startup_app(redis_url: str, mongo_url: str, mongodb_name: str,
                command_palette_path: Union[str, pathlib.Path]):
    conf = init_server_conf(redis_url, mongo_url, mongodb_name,
                            pathlib.Path(command_palette_path))

    return Server(conf)
