from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from cmdproxy.invoke_params import Param


@dataclass
class RunRequest:
    command: Param
    args: Tuple[Param, ...]
    cwd: Optional[str] = None
    env: Optional[Dict[str, Param]] = None
    to_downloads: Optional[List[Param]] = None
    to_uploads: Optional[List[Param]] = None
    stdout: Optional[Param] = None
    stderr: Optional[Param] = None


@dataclass
class RunResponse:
    return_code: int
    exc: Optional[str]
