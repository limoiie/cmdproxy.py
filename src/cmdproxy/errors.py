class CmdProxyError(Exception):
    def __init__(self, msg: str):
        self.msg = msg

    def __str__(self):
        return self.msg


class ServerEndException(CmdProxyError):
    def __init__(self, exc, return_code=-1):
        self.return_code = return_code
        super().__init__(
            f'Exception raised by server: code {return_code},\n{exc}'
        )
