from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from cmdproxy.invoke_params import ParamBase


@dataclass
class RunRequest:
    command: ParamBase
    args: Tuple[ParamBase, ...]
    cwd: Optional[str] = None
    env: Optional[Dict[str, ParamBase]] = None
    to_downloads: Optional[List[Tuple[str, str]]] = None
    to_uploads: Optional[List[Tuple[str, str]]] = None
    stdout: Optional[ParamBase] = None
    stderr: Optional[ParamBase] = None
