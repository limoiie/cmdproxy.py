import contextlib
import os
import tempfile
from abc import abstractmethod
from types import TracebackType
from typing import ContextManager, Optional, Type, TypeVar

from gridfs import GridFS

from cmdproxy.command_tool import CommandTool, ProxyCommandTool
from cmdproxy.invoke_params import ConfigParam, FormatParam, InCloudFileParam, \
    InFileParam, LocalFileParam, OutCloudFileParam, OutFileParam, \
    RemoteConfigParam, StrParam

T = TypeVar('T')


class InvokeMiddle:
    # todo: prefer context wrapped function instead of class
    class ArgGuard(ContextManager):
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
            if isinstance(arg, dict):
                return {k: wrap(v, k) for k, v in arg.items()}
            if isinstance(arg, list):
                return [wrap(v) for v in arg]
            if isinstance(arg, tuple):
                return (wrap(v) for v in arg)
            if isinstance(arg, set):
                return {wrap(v) for v in arg}

            wrapped = self._wrap_arg(arg, k)
            if isinstance(wrapped, InvokeMiddle.ArgGuard):
                return stack.enter_context(wrapped)
            return wrapped

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
    def _wrap_arg(self, arg: T, key=None) -> T or ArgGuard:
        pass


class ProxyClientEndInvokeMiddle(InvokeMiddle):
    def __init__(self, fs: GridFS):
        self.fs: GridFS = fs

    def _wrap_arg(self, arg: T, key=None) -> T or InvokeMiddle.ArgGuard:
        # todo: register Guard bound by Param, iterate them here
        if isinstance(arg, InFileParam):
            return self.InFileGuard(self.fs, key, arg)
        if isinstance(arg, OutFileParam):
            return self.OutFileGuard(self.fs, key, arg)
        if isinstance(arg, FormatParam):
            return self.FormatGuard(self.wrap_args_rec, arg)
        return self.AnyGuard(arg)

    class AnyGuard(InvokeMiddle.ArgGuard):
        def __init__(self, arg):
            self.arg = arg
            self._value = str(arg)

        def __enter__(self):
            return StrParam(self._value)

        def __exit__(self, __exc_type: Type[BaseException] | None,
                     __exc_value: BaseException | None,
                     __traceback: TracebackType | None) -> bool | None:
            pass

    class InFileGuard(InvokeMiddle.ArgGuard):
        def __init__(self, fs, key: str, infile: InFileParam):
            self.fs: GridFS = fs
            self.key = key
            self.infile = infile
            self.file_id = None

        def __enter__(self):
            if self.infile.is_local():
                # upload local input file to the cloud
                self.file_id = self.infile.upload_(self.fs)
                return self.infile.as_cloud()

            return self.infile

        def __exit__(self, __exc_type: Type[BaseException] | None,
                     __exc_value: BaseException | None,
                     __traceback: TracebackType | None) -> None:
            if self.file_id:
                self.fs.delete(self.file_id)

    class OutFileGuard(InvokeMiddle.ArgGuard):
        def __init__(self, fs, key: str, outfile: OutFileParam):
            self.fs: GridFS = fs
            self.key = key
            self.outfile = outfile

        def __enter__(self):
            return self.outfile.as_cloud()

        def __exit__(self, __exc_type: Type[BaseException] | None,
                     __exc_value: BaseException | None,
                     __traceback: TracebackType | None) -> None:
            if self.outfile.is_local():
                # download local output file, and remove from cloud
                try:
                    file_id = self.outfile.download_(self.fs)
                    self.fs.delete(file_id)
                except FileNotFoundError:
                    pass
                return

            # preserve the cloud file on the cloud
            return

    class FormatGuard(InvokeMiddle.ArgGuard):
        def __init__(self, wrap_args_rec, formats: FormatParam):
            self._wrap_args_rec = wrap_args_rec
            self._format: FormatParam = formats
            self._stack: Optional[contextlib.ExitStack] = None

        def __enter__(self):
            self._stack = contextlib.ExitStack()
            self._format.args = self._wrap_args_rec(self._stack,
                                                    self._format.args)
            return self._format

        def __exit__(self, __exc_type: Type[BaseException] | None,
                     __exc_value: BaseException | None,
                     __traceback: TracebackType | None) -> None:
            self._stack.close()


class ProxyServerEndInvokeMiddle(InvokeMiddle):
    def __init__(self, fs: GridFS):
        self.fs: GridFS = fs

    def _wrap_arg(self, arg: T, key=None) -> T or InvokeMiddle.ArgGuard:
        if isinstance(arg, InFileParam):
            assert isinstance(arg, InCloudFileParam)
            return self.InFileGuard(self.fs, key, arg)
        if isinstance(arg, OutFileParam):
            assert isinstance(arg, OutCloudFileParam)
            return self.OutFileGuard(self.fs, key, arg)
        if isinstance(arg, FormatParam):
            assert isinstance(arg, FormatParam)
            return self.FormatGuard(self.wrap_args_rec, arg)
        return self.AnyGuard(arg)

    class AnyGuard(InvokeMiddle.ArgGuard):
        def __init__(self, arg: StrParam):
            self.arg = arg

        def __enter__(self):
            return self.arg.value

        def __exit__(self, __exc_type: Type[BaseException] | None,
                     __exc_value: BaseException | None,
                     __traceback: TracebackType | None) -> bool | None:
            pass

    class InFileGuard(InvokeMiddle.ArgGuard):
        def __init__(self, fs, key: str, infile: InCloudFileParam):
            self.fs: GridFS = fs
            self.key = key
            self.infile = infile
            self.workspace: None or tempfile.TemporaryDirectory = None
            self.filepath = None

        def __enter__(self):
            """Copy infile from cloud to local and return the local path."""
            hostname, filename = self.infile.hostname, self.infile.filename
            self.workspace = tempfile.TemporaryDirectory(prefix=hostname)
            self.filepath = os.path.join(self.workspace.name, filename)
            self.infile.download(self.fs, self.filepath)
            return self.filepath

        def __exit__(self, __exc_type: Type[BaseException] | None,
                     __exc_value: BaseException | None,
                     __traceback: TracebackType | None) -> None:
            """Remove the temp file under the local path."""
            if self.workspace is not None:
                self.workspace.cleanup()

    class OutFileGuard(InvokeMiddle.ArgGuard):
        def __init__(self, fs, key: str, outfile: OutCloudFileParam):
            self.fs: GridFS = fs
            self.key = key
            self.outfile = outfile
            self.filepath = None

        def __enter__(self):
            """Distribute an empty temp file and return its local path."""
            hostname, filename = self.outfile.hostname, self.outfile.filename
            with tempfile.NamedTemporaryFile('wb+', prefix=hostname,
                                             suffix=filename,
                                             delete=False) as tmp:
                self.filepath = tmp.name
            return self.filepath

        def __exit__(self, __exc_type: Type[BaseException] | None,
                     __exc_value: BaseException | None,
                     __traceback: TracebackType | None) -> None:
            """Copy the file under the local path to cloud."""
            self.outfile.upload(self.fs, self.filepath)
            os.remove(self.filepath)

    class FormatGuard(InvokeMiddle.ArgGuard):
        def __init__(self, wrap_args_rec, formats: FormatParam):
            self._wrap_args_rec = wrap_args_rec
            self._format = formats
            self._stack: Optional[contextlib.ExitStack] = None

        def __enter__(self):
            self._stack = contextlib.ExitStack()
            args = self._wrap_args_rec(self._stack, self._format.args)
            return format(self._format.tmpl, *args)

        def __exit__(self, __exc_type: Type[BaseException] | None,
                     __exc_value: BaseException | None,
                     __traceback: TracebackType | None) -> bool | None:
            pass


class LocalInvokeMiddle(InvokeMiddle):
    def __init__(self, fs):
        self.fs: GridFS = fs

    def _wrap_arg(self, arg: T, key=None) -> T or InvokeMiddle.ArgGuard:
        if isinstance(arg, LocalFileParam):
            return arg.filepath
        if isinstance(arg, InFileParam):
            assert isinstance(arg, InCloudFileParam)
            return ProxyServerEndInvokeMiddle.InFileGuard(self.fs, key, arg)
        if isinstance(arg, OutFileParam):
            assert isinstance(arg, OutCloudFileParam)
            return ProxyServerEndInvokeMiddle.OutFileGuard(self.fs, key, arg)
        return arg


class ConfigInvokeMiddle(InvokeMiddle):
    def __init__(self, config):
        self.config: dict = config

    def _wrap_arg(self, arg: T, key=None) -> T or str:
        if isinstance(arg, ConfigParam):
            if arg.param_key not in self.config:
                raise KeyError(
                    f'Failed to fetch config for key: {arg.param_key}, available '
                    f'config keys are {set(self.config.keys())}.')
            return self.config[arg.param_key]
        return arg


class ConfigProxyInvokeMiddle(ConfigInvokeMiddle):
    def _wrap_arg(self, arg: T, key=None) -> T or str:
        if isinstance(arg, RemoteConfigParam):
            return ConfigParam(arg.param_key)
        return super()._wrap_arg(arg, key)
