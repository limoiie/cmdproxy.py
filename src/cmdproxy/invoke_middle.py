import abc
import contextlib
import dataclasses
import os
import tempfile
from abc import abstractmethod
from typing import Any, Callable, ContextManager, Optional, TypeVar

import autodict
from autoserde import AutoSerde
from gridfs import GridFS
from registry import Registry

from cmdproxy.invoke_params import EnvParam, FormatParam, InFileParam, \
    OutFileParam, ParamBase, RemoteEnvParam, StrParam
from cmdproxy.run_request import RunRequest

T = TypeVar('T')


class InvokeMiddle:
    class ArgGuard(abc.ABC):
        @abstractmethod
        def guard(self, arg, key: str) -> ContextManager:
            pass

    def __call__(self, tool: Callable):
        return self.wrap(tool)

    def wrap(self, tool: Callable) -> Callable:
        """
        Wrap a command for preprocessing its args before calling it.

        :param tool: the command going to be wrapped
        :return: the wrapped command
        """

        def wrapped(*args, **kwargs):
            with contextlib.ExitStack() as stack:
                args, kwargs = self.wrap_args_rec(stack, args), \
                    self.wrap_args_rec(stack, kwargs)
                return tool(*args, **kwargs)

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
        guard_cls: Any = self.ArgGuard.query(
            fn=lambda m: isinstance(arg, m['param'])) or self.AnyGuard

        return guard_cls(self)

    @dataclasses.dataclass
    class ArgGuard(InvokeMiddle.ArgGuard, Registry):
        ctx: 'ProxyClientEndInvokeMiddle'

        def guard(self, arg, key) -> ContextManager[Optional[ParamBase]]:
            pass

    @ArgGuard.register(param=StrParam)
    class AnyGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg, key):
            yield StrParam(arg)

    @ArgGuard.register(param=EnvParam)
    class EnvGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: EnvParam, key):
            yield StrParam(os.getenv(arg.name, ''))

    @ArgGuard.register(param=RemoteEnvParam)
    class RemoteEnvGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: RemoteEnvParam, key):
            yield EnvParam(arg.name)

    @ArgGuard.register(param=InFileParam)
    @dataclasses.dataclass
    class InFileGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: InFileParam, key):
            if arg.is_local():
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
        guard_cls: Any = self.ArgGuard.query(
            fn=lambda m: isinstance(arg, m['param']))

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
            yield StrParam(os.getenv(arg.name, ''))

    @ArgGuard.register(param=InFileParam)
    @dataclasses.dataclass
    class InFileGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: InFileParam, key):
            with tempfile.TemporaryDirectory(prefix=arg.hostname) as workspace:
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
                        arg.upload(self.ctx.fs, filepath)
                        os.remove(filepath)

    @ArgGuard.register(param=FormatParam)
    @dataclasses.dataclass
    class FormatGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: FormatParam, key: str):
            with contextlib.ExitStack() as stack:
                args = self.ctx.wrap_args_rec(stack, arg.args)
                yield arg.tmpl.format(*args)


@dataclasses.dataclass
class PackAndSerializeMiddle:
    fmt: str
    options: Optional[autodict.Options] = None

    def __call__(self, tool: Callable[[str], T]) -> Callable[[Any, ...], T]:
        return self.wrap(tool)

    def wrap(self, tool: Callable[[str], T]) -> Callable[[Any, ...], T]:
        def wrapped(command, args, stdout, stderr, env, cwd):
            run_request = RunRequest(
                command=command,
                args=args,
                stdout=stdout,
                stderr=stderr,
                env=env,
                cwd=cwd,
            )
            return tool(AutoSerde.serialize(
                run_request, fmt=self.fmt, options=self.options))

        return wrapped


@dataclasses.dataclass
class DeserializeAndUnpackMiddle:
    fmt: str
    options: Optional[autodict.Options]

    def __call__(self, tool: Callable) -> Callable[[str], T]:
        return self.wrap(tool)

    def wrap(self, tool: Callable) -> Callable[[str], T]:
        def wrapped(serialized: str):
            run_request = AutoSerde.deserialize(
                body=serialized, cls=RunRequest, fmt=self.fmt,
                options=self.options)
            return tool(
                command=run_request.command,
                args=run_request.args,
                stdout=run_request.stdout,
                stderr=run_request.stderr,
                env=run_request.env,
                cwd=run_request.cwd
            )

        return wrapped
