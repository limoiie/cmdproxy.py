import abc
import contextlib
import dataclasses
import os
import tempfile
import traceback
from abc import abstractmethod
from typing import Any, Callable, ContextManager, Optional, TypeVar

import autodict
from autoserde import AutoSerde
from gridfs import GridFS
from registry import Registry

from cmdproxy.errors import ServerEndException
from cmdproxy.invoke_params import EnvParam, FormatParam, InFileParam, \
    OutFileParam, Param, RemoteEnvParam, StrParam
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
    fs: GridFS

    def _guarder(self, arg: T, key=None) -> 'ArgGuard':
        arg_cls = type(arg)
        guard_cls: Any = self.ArgGuard.query(
            fn=lambda m: issubclass(arg_cls, m['param'])) or self.AnyGuard

        return guard_cls(self)

    @dataclasses.dataclass
    class ArgGuard(InvokeMiddle.ArgGuard, Registry):
        ctx: 'ProxyClientEndInvokeMiddle'

        def guard(self, arg, key) -> ContextManager[Optional[Param]]:
            pass

    @ArgGuard.register(param=StrParam)
    class AnyGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg, key):
            yield Param.str(arg)

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
                file_id = arg.upload_(self.ctx.fs)
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
    fs: GridFS

    def _guarder(self, arg: T, key=None) -> 'ArgGuard':
        arg_cls = type(arg)
        guard_cls: Any = self.ArgGuard.query(
            fn=lambda m: issubclass(arg_cls, m['param'])) or self.AnyGuard

        return guard_cls(self)

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
            val = os.getenv(arg.name, None)
            if val is None:
                raise KeyError(f'No environment variable named as {arg.name}.')
            yield val

    @ArgGuard.register(param=InFileParam)
    @dataclasses.dataclass
    class InFileGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: InFileParam, key):
            with tempfile.TemporaryDirectory(prefix=arg.hostname) as workspace:
                logger.debug(
                    f'Downloading cloud input {arg.filepath} uploaded by client '
                    f'to {arg.as_cloud()}...')

                # download from cloud to local temp path
                filepath = os.path.join(workspace, arg.filename)
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
                            f'Uploading local output {arg.filepath} to '
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

            if run_response.exc is not None:
                raise ServerEndException(run_response.exc)

            return run_response.return_code

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

            run_request = AutoSerde.deserialize(
                body=serialized_request, cls=RunRequest, fmt=self.fmt,
                options=self.options)

            try:
                ret_code = func(
                    command=run_request.command,
                    args=run_request.args,
                    stdout=run_request.stdout,
                    stderr=run_request.stderr,
                    env=run_request.env,
                    cwd=run_request.cwd
                )
                full_exc = None
                exc = None

            except Exception as e:
                ret_code = -1
                full_exc = traceback.format_exc()
                exc = e

            if exc or full_exc:
                logger.warning(f'Failed to run the command: {exc}\n{full_exc}')

            run_response = RunResponse(ret_code, full_exc)
            serialized = AutoSerde.serialize(run_response, fmt=self.fmt,
                                             options=self.options)

            return serialized

        return wrapped
