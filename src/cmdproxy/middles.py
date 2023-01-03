import abc
import contextlib
import dataclasses
import os
import pprint
import shutil
import tempfile
import traceback
from abc import abstractmethod
from typing import Any, Callable, ContextManager, Dict, Optional, TypeVar

import autodict
from autoserde import AutoSerde
from gridfs import GridFS
from registry import Registry

from cmdproxy.celery_app.config import CloudFSConf
from cmdproxy.errors import ServerEndException
from cmdproxy.invoke_params import CmdNameParam, CmdPathParam, EnvParam, \
    FormatParam, InFileParam, InStreamParam, OutFileParam, OutStreamParam, \
    Param, RemoteEnvParam, StrParam
from cmdproxy.logging import get_logger
from cmdproxy.protocol import RunRequest, RunResponse

T = TypeVar('T')

logger = get_logger(__name__)


class Middle:
    @abstractmethod
    def wrap(self, func: Callable) -> Callable:
        pass


class InvokeMiddle(Middle):
    class ArgGuard(abc.ABC):
        @abstractmethod
        def guard(self, arg, key: str) -> ContextManager:
            pass

    def __call__(self, func: Callable):
        return self.wrap(func)

    def wrap(self, func: Callable) -> Callable:
        """
        Wrap a command for preprocessing its args before calling it.

        :param func: the command going to be wrapped
        :return: the wrapped command
        """

        def wrapped(*args, **kwargs):
            with contextlib.ExitStack() as stack:
                args, kwargs = self.wrap_args_rec(stack, args), \
                    self.wrap_args_rec(stack, kwargs)
                return func(*args, **kwargs)

        return wrapped

    def wrap_args_rec(self, stack: contextlib.ExitStack, args):
        def wrap(arg, k=None):
            if arg is None:
                return arg

            cls = type(arg)
            if issubclass(cls, dict):
                return cls({k: wrap(v, k) for k, v in arg.items()})
            if issubclass(cls, list):
                return cls(wrap(v) for v in arg)
            if issubclass(cls, tuple):
                return cls(wrap(v) for v in arg)
            if issubclass(cls, set):
                return cls(wrap(v) for v in arg)

            guarder = self._guarder(arg, k)
            return stack.enter_context(guarder.guard(arg, k))

        return wrap(args)

    @abstractmethod
    def _guarder(self, arg: T, key=None) -> ArgGuard:
        pass


@dataclasses.dataclass
class ProxyClientEndInvokeMiddle(InvokeMiddle):
    @dataclasses.dataclass
    class Config:
        cloud: CloudFSConf

    conf: Config

    def _guarder(self, arg: T, key=None) -> 'ArgGuard':
        arg_cls = type(arg)
        guard_cls: Any = self.ArgGuard.query(
            fn=lambda m: issubclass(arg_cls, m['param'])) or self.AnyGuard

        return guard_cls(self)

    @property
    def fs(self) -> GridFS:
        return self.conf.cloud.grid_fs()

    @dataclasses.dataclass
    class ArgGuard(InvokeMiddle.ArgGuard, Registry):
        ctx: 'ProxyClientEndInvokeMiddle'

        @abstractmethod
        def guard(self, arg, key) -> ContextManager[Optional[Param]]:
            pass

    @ArgGuard.register(param=StrParam)
    class AnyGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg, key):
            if isinstance(arg, StrParam):
                yield arg
                return

            assert isinstance(arg, (str, int, float, bool)), \
                f'Arg guarded by AnyGuard should be an instance of any type ' \
                f'of (str, int, float, bool), but get {arg}.'
            yield Param.str(str(arg))

    @ArgGuard.register(param=EnvParam)
    class EnvGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: EnvParam, key):
            val = os.getenv(arg.name, None)
            if val is None:
                raise KeyError(f'No environment variable named as {arg.name}.')
            yield Param.str(val)

    @ArgGuard.register(param=RemoteEnvParam)
    class RemoteEnvGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: RemoteEnvParam, key):
            yield Param.env(arg.name)

    @ArgGuard.register(param=CmdNameParam)
    class CmdNameGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: CmdNameParam, key):
            yield arg

    @ArgGuard.register(param=CmdPathParam)
    class CmdPathGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: CmdPathParam, key):
            yield arg

    @ArgGuard.register(param=InStreamParam)
    class InStreamGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: InStreamParam, key):
            param = Param.ipath(arg.filename).as_cloud()
            param.upload(fs=self.ctx.fs, body=arg.read_bytes())
            try:
                yield param

            finally:
                param.remove_from_cloud(fs=self.ctx.fs)

    @ArgGuard.register(param=OutStreamParam)
    class OutStreamGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: OutStreamParam, key):
            param = Param.opath(arg.filename).as_cloud()
            try:
                yield param

            finally:
                origin_n = arg.io.tell()
                param.download(fs=self.ctx.fs, dst=arg.io)
                param.remove_from_cloud(fs=self.ctx.fs)
                arg.io.seek(origin_n)

    @ArgGuard.register(param=InFileParam)
    @dataclasses.dataclass
    class InFileGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: InFileParam, key):
            if arg.is_local():
                logger.debug(
                    f'Uploading local input {arg.filepath} to '
                    f'{arg.as_cloud()}...')

                # upload local input file to the cloud
                file_id = arg.upload_(fs=self.ctx.fs)
                try:
                    yield arg.as_cloud()

                finally:
                    self.ctx.fs.delete(file_id)

            else:
                yield arg

    @ArgGuard.register(param=OutFileParam)
    @dataclasses.dataclass
    class OutFileGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: OutFileParam, key):
            if arg.is_local():
                try:
                    yield arg.as_cloud()

                finally:
                    with contextlib.suppress(FileNotFoundError):
                        logger.debug(
                            f'Downloading cloud output {arg.as_cloud()} uploaded '
                            f'by server to {arg.filepath}...')

                        # download local output file, and remove from cloud
                        file_id = arg.download_(self.ctx.fs)
                        self.ctx.fs.delete(file_id)

            else:
                yield arg

    @ArgGuard.register(param=FormatParam)
    @dataclasses.dataclass
    class FormatGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: FormatParam, key):
            with contextlib.ExitStack() as stack:
                # wrap the args recursively
                arg.args = self.ctx.wrap_args_rec(stack, arg.args)
                yield arg


@dataclasses.dataclass
class ProxyServerEndInvokeMiddle(InvokeMiddle):
    @dataclasses.dataclass
    class Config:
        cloud: CloudFSConf
        command_palette: Dict[str, str]

    conf: Config

    def _guarder(self, arg: T, key=None) -> 'ArgGuard':
        arg_cls = type(arg)
        guard_cls: Any = self.ArgGuard.query(
            fn=lambda m: issubclass(arg_cls, m['param'])) or self.AnyGuard

        return guard_cls(self)

    @property
    def fs(self) -> GridFS:
        return self.conf.cloud.grid_fs()

    @dataclasses.dataclass
    class ArgGuard(InvokeMiddle.ArgGuard, Registry):
        ctx: 'ProxyServerEndInvokeMiddle'

        def guard(self, arg, key) -> ContextManager[Optional[str]]:
            pass

    @ArgGuard.register(param=StrParam)
    @dataclasses.dataclass
    class AnyGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: StrParam, key):
            yield arg.value

    @ArgGuard.register(param=EnvParam)
    class EnvGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: EnvParam, key):
            val = os.getenv(arg.name)
            if val is None:
                raise KeyError(f'Env var `{arg.name}` not found')
            yield val

    @ArgGuard.register(param=CmdNameParam)
    class CmdNameGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: CmdNameParam, key):
            val = self.ctx.conf.command_palette.get(arg.name)
            if val is None:
                raise KeyError(
                    f'Command `{arg.name}` not found in command-palette:\n'
                    f'{pprint.pformat(self.ctx.conf.command_palette)}')
            yield val

    @ArgGuard.register(param=CmdPathParam)
    class CmdPathGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: CmdPathParam, key):
            if not os.path.exists(arg.path) and not shutil.which(arg.path):
                raise FileNotFoundError(
                    f'Command `{arg.path}` neither exists nor found in system '
                    f'path.')

            yield arg.path

    @ArgGuard.register(param=InFileParam)
    @dataclasses.dataclass
    class InFileGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: InFileParam, key):
            with tempfile.TemporaryDirectory(prefix=arg.hostname) as workspace:
                filepath = os.path.join(workspace, arg.filename)

                logger.debug(
                    f'Downloading cloud input {arg.as_cloud()} uploaded by client '
                    f'to {filepath}...')

                # download from cloud to local temp path
                arg.download(self.ctx.fs, filepath)

                yield filepath

    @ArgGuard.register(param=OutFileParam)
    @dataclasses.dataclass
    class OutFileGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: OutFileParam, key):
            """Distribute an empty temp file and return its local path."""
            with tempfile.TemporaryDirectory(prefix=arg.hostname) as workspace:
                filepath = os.path.join(workspace, arg.filename)
                try:
                    yield filepath

                finally:
                    if os.path.exists(filepath):
                        logger.debug(
                            f'Uploading local output {filepath} to '
                            f'{arg.as_cloud()}...')

                        arg.upload(self.ctx.fs, filepath)
                        os.remove(filepath)

    @ArgGuard.register(param=FormatParam)
    @dataclasses.dataclass
    class FormatGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: FormatParam, key: str):
            with contextlib.ExitStack() as stack:
                args = self.ctx.wrap_args_rec(stack, arg.args)
                yield arg.tmpl.format(**args)


@dataclasses.dataclass
class PackAndSerializeMiddle(Middle):
    fmt: str
    options: Optional[autodict.Options] = None

    def __call__(self, func: Callable[[str], str]) -> Callable[..., int]:
        return self.wrap(func)

    def wrap(self, func: Callable[[str], str]) -> Callable[..., int]:
        def wrapped(command, args, stdout, stderr, env, cwd):
            run_request = RunRequest(
                command=command,
                args=args,
                stdout=stdout,
                stderr=stderr,
                env=env,
                cwd=cwd,
            )

            serialized_response = func(AutoSerde.serialize(
                run_request, fmt=self.fmt, options=self.options))
            run_response = AutoSerde.deserialize(
                body=serialized_response, cls=RunResponse, fmt=self.fmt,
                options=self.options)

            return_code = run_response.return_code
            if run_response.exc is not None:
                raise ServerEndException(run_response.exc)

            return return_code

        return wrapped


@dataclasses.dataclass
class DeserializeAndUnpackMiddle(Middle):
    fmt: str
    options: Optional[autodict.Options]

    def __call__(self, func: Callable) -> Callable[[str], str]:
        return self.wrap(func)

    def wrap(self, func: Callable) -> Callable[[str], str]:
        def wrapped(serialized_request: str):
            logger.debug(f'Received request: {serialized_request}')

            ret_code, exc = -1, None
            try:
                run_request = AutoSerde.deserialize(
                    body=serialized_request, cls=RunRequest, fmt=self.fmt,
                    options=self.options)

                ret_code = func(
                    command=run_request.command,
                    args=run_request.args,
                    stdout=run_request.stdout,
                    stderr=run_request.stderr,
                    env=run_request.env,
                    cwd=run_request.cwd
                )

            except Exception as e:
                exc = f'{repr(e)}:\n{traceback.format_exc()}'
                logger.warning(f'Exception raised when running command: {exc}')

            run_response = RunResponse(ret_code, exc)
            serialized = AutoSerde.serialize(run_response, fmt=self.fmt,
                                             options=self.options)

            return serialized

        return wrapped
