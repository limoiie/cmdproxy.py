import celery
from autoserde import AutoSerde

from cmdproxy.celery_app.config import init_client_end_conf, CmdProxyClientConf
from cmdproxy.invoke_middle import ProxyClientEndInvokeMiddle
from cmdproxy.run_request import RunRequest
from cmdproxy.singleton import Singleton


class Client(Singleton):
    def __init__(self, conf: CmdProxyClientConf):
        # todo: resolve config or environment vars?
        @ProxyClientEndInvokeMiddle(conf.celery.grid_fs())
        def proxy(command, args, stdout, stderr, env, cwd):
            # all the args has been converted into strings
            from cmdproxy.celery_app.tasks import run
            assert isinstance(run, celery.Task)

            request = RunRequest(
                command=command,
                stdout=stdout,
                stderr=stderr,
                env=env,
                cwd=cwd,
                args=args,
            )
            return run.delay(AutoSerde.serialize(request))

        self._proxy = proxy
        self._conf = conf

    def run(self, command, *args, stdout=None, stderr=None, env=None, cwd=None):
        return self._proxy(
            command=command,
            args=args,
            stdout=stdout,
            stderr=stderr,
            env=env,
            cwd=cwd
        )


def startup_app(redis_url, mongo_url, mongodb_name='cmdproxy'):
    conf = init_client_end_conf(redis_url, mongo_url, mongodb_name)

    Client(conf)
