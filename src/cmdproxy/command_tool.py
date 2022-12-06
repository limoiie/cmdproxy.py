import dataclasses
import os
import pathlib
import subprocess
from typing import Optional

from flexio import FlexBinaryIO

LOG_PREFIX = 'bin2sth.bin2data.analysis'


@dataclasses.dataclass
class CommandToolErr:
    code: int = -1
    err: pathlib.Path | str = ''
    out: pathlib.Path | str = ''

    def is_err_message(self):
        return self.__is_message(self.err)

    def is_out_message(self):
        return self.__is_message(self.out)

    @staticmethod
    def __is_message(data):
        if os.path.exists(data):
            return False
        return isinstance(data, (str, bytes))


class CommandTool:
    def __init__(self, command):
        self._command: str = str(command)
        self._err: Optional[CommandToolErr] = None

    def __call__(self, *args, stdout, stderr=None, env=None, cwd=None) -> int:
        """
        Launch the command tool specified by :var self._cmd_path.
        The error information will be collected into :var self._err_list.

        :param args: Extra parameters
        :param stdout: The path where the stdout should be directed to.
        :param stderr: The path where the stderr should be directed to.
        :param env: A dict for defining environment variables
        :param cwd: Target working directory.
        :return: process exit code
        """
        with FlexBinaryIO(stdout, mode='wb+') as out, \
                FlexBinaryIO(stderr, mode='wb+') as err:
            res = subprocess.run([self._command, *args], env=env, cwd=cwd,
                                 stdout=out, stderr=err)
            if res.returncode != 0:
                # todo: should stdout be a message string?
                self._err = CommandToolErr(res.returncode, stdout, stderr)
        return res.returncode

    def err(self):
        return self._err

    def cmd(self):
        return os.path.basename(self._command)

    def cmd_path(self):
        return pathlib.Path(self._command)


class ProxyCommandTool(CommandTool):
    def __init__(self, proxy_tool: CommandTool):
        super().__init__(proxy_tool.cmd_path())
        self.proxy_tool = proxy_tool

    def __call__(self, *args, stdout, stderr=None, env=None, cwd=None):
        return self.proxy_tool(*args, stdout=stdout, stderr=stderr, env=env,
                               cwd=cwd)

    def err(self):
        return self.proxy_tool.err()

    def cmd(self):
        return self.proxy_tool.cmd()

    def cmd_path(self):
        return self.proxy_tool.cmd_path()
