import abc
import contextlib
import dataclasses
import os
import tempfile
from abc import abstractmethod
from typing import Any, ContextManager, TypeVar

from gridfs import GridFS
from registry import Registry

from cmdproxy.command_tool import CommandTool, ProxyCommandTool
from cmdproxy.invoke_params import ConfigParam, FormatParam, InFileParam, \
    OutFileParam, ParamBase, RemoteConfigParam, StrParam

T = TypeVar('T')


class InvokeMiddle:
    class ArgGuard(abc.ABC):
        @abstractmethod
        def guard(self, arg, key: str) -> ContextManager:
            pass

    class WrappedCommandTool(ProxyCommandTool):
        """
        Call the proxy tool after preprocessing all its arguments.
        """

        def __init__(self, proxy_tool: CommandTool, fn_wrap_args):
            super().__init__(proxy_tool)
            self.__fn_wrap_args = fn_wrap_args

        def __call__(self, *args, **kwargs):
            with self.__guard(args, kwargs) as (args, kwargs):
                return self.proxy_tool(*args, **kwargs)

        @contextlib.contextmanager
        def __guard(self, args, kwargs):
            with contextlib.ExitStack() as stack:
                yield self.__fn_wrap_args(stack, args), \
                    self.__fn_wrap_args(stack, kwargs)

    def wrap_args_rec(self, stack: contextlib.ExitStack, args):
        def wrap(arg, k=None):
            if arg is None:
                return arg
            if isinstance(arg, dict):
                return {k: wrap(v, k) for k, v in arg.items()}
            if isinstance(arg, list):
                return [wrap(v) for v in arg]
            if isinstance(arg, tuple):
                return (wrap(v) for v in arg)
            if isinstance(arg, set):
                return {wrap(v) for v in arg}

            guarder = self._guarder(arg, k)
            return stack.enter_context(guarder.guard(arg, k))

        return wrap(args)

    def __call__(self, tool: CommandTool):
        return self.wrap(tool)

    def wrap(self, tool: CommandTool) -> CommandTool:
        """
        Wrap a command for preprocessing its args before calling it.

        :param tool: the command going to be wrapped
        :return: the wrapped command
        """
        return InvokeMiddle.WrappedCommandTool(tool, self.wrap_args_rec)

    @abstractmethod
    def _guarder(self, arg: T, key=None) -> ArgGuard:
        pass


class ProxyClientEndInvokeMiddle(InvokeMiddle):
    def __init__(self, fs: GridFS):
        self.fs: GridFS = fs

    def _guarder(self, arg: T, key=None) -> 'ArgGuard':
        guard_cls: Any = self.ArgGuard.query(
            fn=lambda m: isinstance(arg, m['param'])) or self.AnyGuard

        return guard_cls(self)

    @dataclasses.dataclass
    class ArgGuard(InvokeMiddle.ArgGuard, Registry):
        ctx: 'ProxyClientEndInvokeMiddle'

        def guard(self, arg, key) -> ContextManager[ParamBase or None]:
            pass

    @ArgGuard.register(param=StrParam)
    class AnyGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg, key):
            yield StrParam(arg)

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
    class FormatGuard(InvokeMiddle.ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: FormatParam, key):
            with contextlib.ExitStack() as stack:
                # wrap the args recursively
                arg.args = self.ctx.wrap_args_rec(stack, arg.args)
                yield arg


class ProxyServerEndInvokeMiddle(InvokeMiddle):
    def __init__(self, fs: GridFS):
        self.fs: GridFS = fs

    def _guarder(self, arg: T, key=None) -> 'ArgGuard':
        guard_cls: Any = self.ArgGuard.query(
            fn=lambda m: isinstance(arg, m['param']))

        return guard_cls(self)

    @dataclasses.dataclass
    class ArgGuard(InvokeMiddle.ArgGuard, Registry):
        ctx: 'ProxyServerEndInvokeMiddle'

        def guard(self, arg, key) -> ContextManager[str or None]:
            pass

    @ArgGuard.register(param=StrParam)
    @dataclasses.dataclass
    class AnyGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: StrParam, key):
            yield arg.value

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
    class FormatGuard(InvokeMiddle.ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: FormatParam, key: str):
            with contextlib.ExitStack() as stack:
                args = self.ctx.wrap_args_rec(stack, arg.args)
                yield format(arg.tmpl, *args)


class ConfigInvokeMiddle(InvokeMiddle):
    def __init__(self, config):
        self.config: dict = config

    def _guarder(self, arg: T, key=None) -> T or str:
        if isinstance(arg, ConfigParam):
            if arg.param_key not in self.config:
                raise KeyError(
                    f'Failed to fetch config for key: {arg.param_key}, available '
                    f'config keys are {set(self.config.keys())}.')
            return self.config[arg.param_key]
        return arg


class ConfigProxyInvokeMiddle(ConfigInvokeMiddle):
    def _guarder(self, arg: T, key=None) -> T or str:
        if isinstance(arg, RemoteConfigParam):
            return ConfigParam(arg.param_key)
        return super()._guarder(arg, key)
