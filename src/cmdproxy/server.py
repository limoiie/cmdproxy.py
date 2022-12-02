import pathlib

from autoserde import AutoSerde

from cmdproxy.celery_app.config import init_server_end_conf, CmdProxyServerConf
from cmdproxy.command_tool import CommandTool
from cmdproxy.invoke_middle import ProxyServerEndInvokeMiddle
from cmdproxy.run_request import RunRequest
from cmdproxy.singleton import Singleton


class Server(Singleton):
    def __init__(self, conf: CmdProxyServerConf):
        # todo: resolve config or environment vars?
        @ProxyServerEndInvokeMiddle(conf.celery.grid_fs())
        def proxy(command, args, stdout, stderr, env, cwd):
            return CommandTool(command)(
                *args,
                stdout=stdout,
                stderr=stderr,
                env=env,
                cwd=cwd
            )

        self._proxy = proxy
        self._conf = conf

    def run(self, serialized_request: str):
        request = AutoSerde.deserialize(serialized_request, RunRequest)
        # all the args has been converted into strings

        return self._proxy(
            command=self._conf.command_palette[request.command],
            args=request.args,
            stdout=request.stdout,
            stderr=request.stderr,
            env=request.env,
            cwd=request.cwd
        )


def startup_app(redis_url: str, mongo_url: str, mongodb_name: str,
                command_palette_path: str):
    conf = init_server_end_conf(redis_url, mongo_url, mongodb_name,
                                pathlib.Path(command_palette_path))

    Server(conf)
