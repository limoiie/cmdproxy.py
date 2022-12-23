import dataclasses
import pathlib
import re
from abc import ABC, abstractmethod
from pathlib import Path
from socket import gethostname
from typing import Dict, Optional, Tuple, TypeVar, Union

import autodict
import flexio
import parse
from autodict import AutoDict, Dictable
from autodict.predefined import dataclass_from_dict, dataclass_to_dict
from bson import ObjectId
from flexio.flexio import FilePointer
from gridfs import GridFS, GridOut

from cmdproxy.logging import get_logger

logger = get_logger(__name__)


class Param(Dictable):
    def _to_dict(self, options: autodict.Options) -> dict:
        cls = type(self)
        sub_name = AutoDict.meta_of(cls).name
        return {sub_name: self._subclass_to_dict(options=options)}

    @classmethod
    def _from_dict(cls, obj: dict, options: autodict.Options) -> 'DerivedParam':
        if cls is Param:
            sub_name, sub_obj = obj.popitem()
            sub_cls = AutoDict.query(name=sub_name)
            return AutoDict.from_dict(sub_obj, cls=sub_cls, options=options)

        sub_name = AutoDict.meta_of(cls).name
        sub_obj = obj[sub_name] if sub_name in obj else obj
        return cls._subclass_from_dict(sub_obj, options)

    def _subclass_to_dict(self, options: autodict.Options):
        return dataclass_to_dict(self, options)

    @classmethod
    def _subclass_from_dict(cls, obj: dict, options: autodict.Options):
        return dataclass_from_dict(cls, obj, options)

    @staticmethod
    def env(name: str) -> 'EnvParam':
        return EnvParam(name=name)

    @staticmethod
    def remote_env(name: str) -> 'RemoteEnvParam':
        return RemoteEnvParam(name=name)

    @staticmethod
    def ipath(ref: Union[str, pathlib.Path]) \
            -> Union['InLocalFileParam', 'InCloudFileParam']:
        """
        Create either an InLocalFileParam or an InCloudFileParam according to url.

        :param ref: Either being a filepath (for local), or a cloud_url (for cloud).
        :return: A sub instance of InFileParam.
        """
        return _file(ref, is_input=True)

    @staticmethod
    def opath(ref: Union[str, pathlib.Path]) \
            -> Union['OutLocalFileParam', 'OutCloudFileParam']:
        """
        Create either an OutLocalFileParam or an OutCloudFileParam according to url.

        :param ref: Either being a filepath (for local), or a cloud_url (for cloud).
        :return: A sub instance of OutFileParam.
        """
        return _file(ref, is_input=False)

    @staticmethod
    def format(tmpl, args):
        return FormatParam(tmpl=tmpl, args=args)

    @staticmethod
    def str(value: str) -> 'StrParam':
        return StrParam(value=value)


DerivedParam = TypeVar('DerivedParam', bound=Param)


@dataclasses.dataclass
class EnvParam(Param):
    name: str

    def __str__(self):
        return self.name


@dataclasses.dataclass
class RemoteEnvParam(Param):
    name: str

    def __str__(self):
        return self.name


LINK_REGEX = re.compile(r'<#:([io])>(.+?)</>')
CLOUD_URL_PATTERN = '@{hostname}:{abspath}'
LOCAL_HOSTNAME = gethostname()


@dataclasses.dataclass
class FileParamBase(Param):
    filepath: Path
    hostname: str = LOCAL_HOSTNAME

    def __post_init__(self):
        self.filepath = Path(self.filepath)

        if self.is_local():
            assert self.hostname == LOCAL_HOSTNAME

    def is_input(self):
        return isinstance(self, InFileParam)

    def is_output(self):
        return not self.is_input()

    @abstractmethod
    def as_cloud(self):
        return NotImplemented

    def is_cloud(self):
        return isinstance(self, CloudFileParam)

    def is_local(self):
        return not self.is_cloud()

    @property
    def cloud_url(self):
        return CLOUD_URL_PATTERN.format(hostname=self.hostname,
                                        abspath=self.filepath.as_posix())

    @property
    def filename(self):
        return str(self.filepath.name)

    def id_on_cloud(self, fs: GridFS) -> ObjectId or None:
        try:
            # noinspection PyProtectedMember
            return self.find_on_cloud(fs)._id
        except FileNotFoundError:
            return None

    def exists_on_cloud(self, fs: GridFS) -> bool:
        return fs.exists(dict(filename=self.cloud_url))

    def find_on_cloud(self, fs: GridFS) -> GridOut:
        f = fs.find_one(dict(filename=self.cloud_url))
        if f is None:
            raise FileNotFoundError(f'No cloud file with name {self.cloud_url}')
        return f

    # noinspection PyProtectedMember
    def remove_from_cloud(self, fs: GridFS):
        f = fs.find_one(dict(filename=self.cloud_url))
        if f is not None:
            fs.delete(f._id)
        return f._id

    def download(self, fs: GridFS, fp: Optional[FilePointer] = None) \
            -> Tuple[ObjectId, Optional[bytes]]:
        with flexio.FlexBinaryIO(fp, 'wb+') as tgt:
            with self.find_on_cloud(fs) as src:
                tgt.write(src.read())

            if tgt.in_mem:
                tgt.seek(0)
                # noinspection PyProtectedMember
                return src._id, tgt.read()

            # noinspection PyProtectedMember
            return src._id, None

    def upload(self, fs: GridFS, fp: Optional[FilePointer] = None,
               body: Optional[bytes] = None) -> ObjectId:
        with flexio.FlexBinaryIO(fp, 'rb', init=body) as src:
            return fs.put(src, filename=self.cloud_url)

    @abstractmethod
    def download_(self, fs: GridFS) -> ObjectId:
        """Download in the place."""
        pass

    @abstractmethod
    def upload_(self, fs: GridFS) -> ObjectId:
        """Upload in the place."""
        pass


class CloudFileParam(FileParamBase, ABC):
    def is_cloud_only(self):
        """
        A file param is cloud-only only if the filepath is relative.
        """
        return self.filepath.parts[0] != '/'

    def is_local_bind(self):
        return not self.is_cloud_only() and self.hostname == gethostname()

    def alloc_(self, fs: GridFS):
        # todo: make sure this url is unique on the cloud
        pass

    def download_(self, fs: GridFS) -> ObjectId:
        if self.is_local_bind():
            return self.download(fs, self.filepath)[0]
        raise RuntimeError('Only local-bind cloud file can download to itself.')

    def upload_(self, fs: GridFS) -> ObjectId:
        if self.is_local_bind():
            return self.upload(fs, self.filepath)
        raise RuntimeError('Only local-bind cloud file can upload itself.')


class LocalFileParam(FileParamBase, ABC):
    def download_(self, fs: GridFS) -> ObjectId:
        assert self.hostname == gethostname()
        return self.download(fs, self.filepath)[0]

    def upload_(self, fs: GridFS) -> ObjectId:
        assert self.hostname == gethostname()
        return self.upload(fs, self.filepath)


class InFileParam(FileParamBase, ABC):
    def as_cloud(self) -> 'InCloudFileParam':
        return InCloudFileParam(self.filepath, self.hostname)


class OutFileParam(FileParamBase, ABC):
    def as_cloud(self) -> 'OutCloudFileParam':
        return OutCloudFileParam(self.filepath, self.hostname)


class InCloudFileParam(CloudFileParam, InFileParam):
    def as_cloud(self) -> 'InCloudFileParam':
        return self


class InLocalFileParam(LocalFileParam, InFileParam):
    pass


class OutCloudFileParam(CloudFileParam, OutFileParam):
    def as_cloud(self) -> 'OutCloudFileParam':
        return self


class OutLocalFileParam(LocalFileParam, OutFileParam):
    pass


@dataclasses.dataclass
class FormatParam(Param):
    tmpl: str
    args: Dict[str, Param]


@dataclasses.dataclass
class StrParam(Param):
    value: str


def upload_as_in(fs: GridFS, path: Union[str, Path]) -> InCloudFileParam:
    param = Param.ipath(path).as_cloud()
    param.upload_(fs)
    return param


def alloc_as_out(fs: GridFS, path: Union[str, Path]) -> OutCloudFileParam:
    param = Param.opath(path).as_cloud()
    param.alloc_(fs)
    return param


def _file(url, is_input):
    if isinstance(url, Path):
        return InLocalFileParam(url) if is_input else OutLocalFileParam(url)

    m = parse.parse(CLOUD_URL_PATTERN, url)
    if not m:
        return InLocalFileParam(url) if is_input else OutLocalFileParam(url)

    hostname, filepath = m['hostname'], m['abspath']

    return InCloudFileParam(filepath, hostname) if is_input else \
        OutCloudFileParam(filepath, hostname)
