from dataclasses import dataclass
from typing import Optional

from autoserde import serdeable


@serdeable
@dataclass
class RunRequest:
    command: str
    args: tuple[str, ...]
    cwd: Optional[str] = None
    env: Optional[dict] = None
    to_downloads: Optional[list[(str, str)]] = None
    to_uploads: Optional[list[(str, str)]] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None
