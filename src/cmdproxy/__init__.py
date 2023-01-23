from cmdproxy.celery_app.config import init_client_conf
from cmdproxy.client import Client
from cmdproxy.invoke_params import Param

try:
    import importlib.metadata as _importlib_metadata
except ModuleNotFoundError:
    # noinspection PyUnresolvedReferences
    import importlib_metadata as _importlib_metadata

__all__ = ['__version__', 'init_client_conf', 'Client', 'Param']

try:
    __version__ = _importlib_metadata.version("cmdproxy")
except _importlib_metadata.PackageNotFoundError:
    __version__ = "unknown version"
