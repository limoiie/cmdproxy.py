import dataclasses
import os
import pathlib
import subprocess
from typing import Optional

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
    def __init__(self, command_path):
        self._err: Optional[CommandToolErr] = None
        self._cmd_path = pathlib.Path(command_path)

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
        res = subprocess.run(args, executable=self._cmd_path, env=env, cwd=cwd,
                             stdout=stdout, stderr=stderr)
        if res.returncode != 0:
            # todo: should stdout be a message string?
            self._err = CommandToolErr(res.returncode, stdout, stderr)
        return res.returncode

    def err(self):
        return self._err

    def cmd(self):
        return self._cmd_path.name

    def cmd_path(self):
        return self._cmd_path


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
