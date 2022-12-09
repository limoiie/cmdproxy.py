from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from cmdproxy.invoke_params import ParamBase


@dataclass
class RunRequest:
    command: ParamBase
    args: Tuple[ParamBase, ...]
    cwd: Optional[str] = None
    env: Optional[Dict[str, ParamBase]] = None
    to_downloads: Optional[List[ParamBase]] = None
    to_uploads: Optional[List[ParamBase]] = None
    stdout: Optional[ParamBase] = None
    stderr: Optional[ParamBase] = None


@dataclass
class RunResponse:
    return_code: int
    exc: Optional[str]
