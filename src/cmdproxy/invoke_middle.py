import abc
import contextlib
import dataclasses
import os
import tempfile
from abc import abstractmethod
from typing import Callable, ContextManager, List, TypeVar

from gridfs import GridFS

from cmdproxy.command_tool import CommandTool, ProxyCommandTool
from cmdproxy.invoke_params import ConfigParam, FormatParam, InCloudFileParam, \
    InFileParam, OutCloudFileParam, OutFileParam, ParamBase, RemoteConfigParam, \
    StrParam

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
        # todo: register Guard bound by Param, iterate them here
        if isinstance(arg, InFileParam):
            return self.InFileGuard(self.fs)
        if isinstance(arg, OutFileParam):
            return self.OutFileGuard(self.fs)
        if isinstance(arg, FormatParam):
            return self.FormatGuard(self.wrap_args_rec)
        return self.AnyGuard()

    class ArgGuard(InvokeMiddle.ArgGuard):
        def guard(self, arg, key) -> ContextManager[ParamBase or None]:
            pass

    class AnyGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg, key):
            yield StrParam(arg)

    @dataclasses.dataclass
    class InFileGuard(ArgGuard):
        fs: GridFS

        @contextlib.contextmanager
        def guard(self, arg: InFileParam, key):
            if arg.is_local():
                # upload local input file to the cloud
                file_id = arg.upload_(self.fs)
                try:
                    yield arg.as_cloud()

                finally:
                    self.fs.delete(file_id)

            else:
                yield arg

    @dataclasses.dataclass
    class OutFileGuard(ArgGuard):
        fs: GridFS

        @contextlib.contextmanager
        def guard(self, arg: OutFileParam, key):
            if arg.is_local():
                try:
                    yield arg.as_cloud()

                finally:
                    with contextlib.suppress(FileNotFoundError):
                        # download local output file, and remove from cloud
                        file_id = arg.download_(self.fs)

                    self.fs.delete(file_id)

            else:
                yield arg

    @dataclasses.dataclass
    class FormatGuard(InvokeMiddle.ArgGuard):
        wrap_args_rec: Callable[[contextlib.ExitStack, List], List]

        @contextlib.contextmanager
        def guard(self, arg: FormatParam, key):
            with contextlib.ExitStack() as stack:
                # wrap the args recursively
                arg.args = self.wrap_args_rec(stack, arg.args)
                yield arg


class ProxyServerEndInvokeMiddle(InvokeMiddle):
    def __init__(self, fs: GridFS):
        self.fs: GridFS = fs

    def _guarder(self, arg: T, key=None) -> 'ArgGuard':
        if isinstance(arg, InFileParam):
            assert isinstance(arg, InCloudFileParam)
            return self.InFileGuard(self.fs)
        if isinstance(arg, OutFileParam):
            assert isinstance(arg, OutCloudFileParam)
            return self.OutFileGuard(self.fs)
        if isinstance(arg, FormatParam):
            assert isinstance(arg, FormatParam)
            return self.FormatGuard(self.wrap_args_rec)
        return self.AnyGuard()

    class ArgGuard(InvokeMiddle.ArgGuard):
        def guard(self, arg, key) -> ContextManager[str or None]:
            pass

    @dataclasses.dataclass
    class AnyGuard(ArgGuard):
        @contextlib.contextmanager
        def guard(self, arg: StrParam, key):
            yield arg.value

    @dataclasses.dataclass
    class InFileGuard(ArgGuard):
        fs: GridFS

        @contextlib.contextmanager
        def guard(self, arg: InFileParam, key):
            with tempfile.TemporaryDirectory(prefix=arg.hostname) as workspace:
                # download from cloud to local temp path
                filepath = os.path.join(workspace, arg.filename)
                arg.download(self.fs, filepath)

                yield filepath

    @dataclasses.dataclass
    class OutFileGuard(ArgGuard):
        fs: GridFS

        @contextlib.contextmanager
        def guard(self, arg: OutFileParam, key):
            """Distribute an empty temp file and return its local path."""
            with tempfile.TemporaryDirectory(prefix=arg.hostname) as workspace:
                filepath = os.path.join(workspace, arg.filename)
                try:
                    yield filepath

                finally:
                    if os.path.exists(filepath):
                        arg.upload(self.fs, filepath)
                        os.remove(filepath)

    @dataclasses.dataclass
    class FormatGuard(InvokeMiddle.ArgGuard):
        wrap_args_rec: Callable[[contextlib.ExitStack, List], List]

        @contextlib.contextmanager
        def guard(self, arg: FormatParam, key: str):
            with contextlib.ExitStack() as stack:
                args = self.wrap_args_rec(stack, arg.args)
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
