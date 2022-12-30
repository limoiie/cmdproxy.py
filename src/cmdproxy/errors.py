class CmdProxyError(Exception):
    def __init__(self, msg: str):
        self.msg = msg

    def __str__(self):
        return self.msg


class ServerEndException(CmdProxyError):
    def __init__(self, exc):
        super().__init__(f'ServerEndException: {exc}')
