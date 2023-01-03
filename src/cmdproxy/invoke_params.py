import dataclasses
import io
import os
import pathlib
from abc import ABC, abstractmethod
from socket import gethostname
from typing import Dict, Optional, Tuple, TypeVar, Union

import autodict
import flexio
import parse
from autodict import AutoDict, Dictable
from autodict.predefined import dataclass_from_dict, dataclass_to_dict
from bson import ObjectId
from flexio.flexio import FPOrStrIO
from gridfs import GridFS, GridOut

from cmdproxy.logging import get_logger

logger = get_logger(__name__)

AnyPath = Union[str, os.PathLike]


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
        """Create an env Param that will be resolved at client end."""
        return EnvParam(name=name)

    @staticmethod
    def remote_env(name: str) -> 'RemoteEnvParam':
        """Create an env Param that will be resolved at server end."""
        return RemoteEnvParam(name=name)

    @staticmethod
    def format(tmpl: str, args: dict) -> 'FormatParam':
        """
        Create a format Param that will be flattened at server end.

        The args of this param will be guarded as normal params.
        """
        return FormatParam(tmpl=tmpl, args=args)

    @staticmethod
    def cmd_name(name: str) -> 'CmdNameParam':
        """
        Create a command which is defined in server end's command palette file.

        The command ought to be sent to the queue named with the same name.
        """
        return CmdNameParam(name=name)

    @staticmethod
    def cmd_path(path: str) -> 'CmdPathParam':
        """
        Create a command with the path existing in server end.

        You need to specify the target queue explicitly.
        """
        return CmdPathParam(path=path)

    # noinspection PyShadowingNames
    @staticmethod
    def istream(io: Union[io.BufferedRandom, io.BytesIO], filename: str):
        """
        Create a Param that takes io as the input source.
        """
        return InStreamParam(io, filename)

    # noinspection PyShadowingNames
    @staticmethod
    def ostream(io: Union[io.BufferedRandom, io.BytesIO], filename: str):
        """
        Create a Param that takes io as the output destination.
        """
        return OutStreamParam(io, filename)

    @staticmethod
    def ipath(ref: AnyPath) -> Union['InLocalFileParam', 'InCloudFileParam']:
        """
        Create either an InLocalFileParam or an InCloudFileParam according to ref.

        :param ref: Either being a filepath (for local), or a cloud_url (for cloud).
        :return: A sub instance of InFileParam.
        """
        return _file(ref, is_input=True)

    @staticmethod
    def opath(ref: AnyPath) -> Union['OutLocalFileParam', 'OutCloudFileParam']:
        """
        Create either an OutLocalFileParam or an OutCloudFileParam according to ref.

        :param ref: Either being a filepath (for local), or a cloud_url (for cloud).
        :return: A sub instance of OutFileParam.
        """
        return _file(ref, is_input=False)

    @staticmethod
    def str(value: str) -> 'StrParam':
        """
        Create a Param that wraps the given string value.

        It will be unwrapped to original string value at server end.
        """
        return StrParam(value=value)

    def is_cloud(self):
        return isinstance(self, CloudFileParam)

    def is_local(self):
        return isinstance(self, LocalFileParam)

    def is_stream(self):
        return isinstance(self, StreamParam)

    def is_input(self):
        return isinstance(self, InFileParam)

    def is_output(self):
        return isinstance(self, OutFileParam)


DerivedParam = TypeVar('DerivedParam', bound=Param)


@dataclasses.dataclass
class StrParam(Param):
    value: str

    def __str__(self):
        return self.value


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


@dataclasses.dataclass
class FormatParam(Param):
    tmpl: str
    args: Dict[str, Param]


class CmdParamBase(Param):
    pass


@dataclasses.dataclass
class CmdNameParam(CmdParamBase):
    name: str


@dataclasses.dataclass
class CmdPathParam(CmdParamBase):
    path: str


@dataclasses.dataclass
class StreamParam:
    io: Union[io.BufferedRandom, io.BytesIO]
    filename: str

    def read_bytes(self) -> bytes:
        text = self.io.read()
        return text


@dataclasses.dataclass
class InStreamParam(StreamParam):
    def __post_init__(self):
        assert self.io.readable()


@dataclasses.dataclass
class OutStreamParam(StreamParam):
    def __post_init__(self):
        assert self.io.writable()


CLOUD_URL_PATTERN = '@{hostname}:{abspath}'
LOCAL_HOSTNAME = gethostname()


@dataclasses.dataclass
class FileParamBase(Param):
    filepath: pathlib.Path
    hostname: str = LOCAL_HOSTNAME

    def __post_init__(self):
        self.filepath = pathlib.Path(self.filepath)

        if self.is_local():
            assert self.hostname == LOCAL_HOSTNAME

    @abstractmethod
    def as_cloud(self):
        return NotImplemented

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
        return f._id if f else None

    def download(self, fs: GridFS, dst: Optional[FPOrStrIO] = None,
                 close_io: Optional[bool] = None) \
            -> Tuple[ObjectId, Optional[bytes]]:
        with flexio.flex_open(dst, 'w+b', close_io=close_io) as tgt:
            with self.find_on_cloud(fs) as src:
                tgt.write(src.read())

            if dst is None:
                tgt.seek(0)
                # noinspection PyProtectedMember
                return src._id, tgt.read()

            # noinspection PyProtectedMember
            return src._id, None

    def upload(self, fs: GridFS, src: Optional[FPOrStrIO] = None,
               body: Optional[bytes] = None,
               close_io: Optional[bool] = None) -> ObjectId:
        with flexio.flex_open(src, 'rb', init=body, close_io=close_io) as src:
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

    def alloc_(self, fs: GridFS, force=False) -> ObjectId:
        if self.exists_on_cloud(fs) and not force:
            raise FileExistsError(
                f'File already exists on the cloud: {self.cloud_url}')

        self.remove_from_cloud(fs)
        return fs.put(b'', filename=self.cloud_url)

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
        assert self.hostname == LOCAL_HOSTNAME, \
            f'Exceptional inplace download: Param is created on ' \
            f'`{self.hostname}`, while going to download here `{LOCAL_HOSTNAME}`'
        return self.download(fs, self.filepath)[0]

    def upload_(self, fs: GridFS) -> ObjectId:
        assert self.hostname == LOCAL_HOSTNAME, \
            f'Exceptional inplace upload: Param is created on ' \
            f'`{self.hostname}`, while going to upload here `{LOCAL_HOSTNAME}`'
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


def upload_as_in(fs: GridFS, path: AnyPath) -> InCloudFileParam:
    param = Param.ipath(path).as_cloud()
    param.upload_(fs)
    return param


def alloc_as_out(fs: GridFS, path: AnyPath) -> OutCloudFileParam:
    param = Param.opath(path).as_cloud()
    param.alloc_(fs)
    return param


def _file(url: AnyPath, is_input: bool):
    if isinstance(url, os.PathLike):
        url = pathlib.Path(url)
        return InLocalFileParam(url) if is_input else OutLocalFileParam(url)

    m = parse.parse(CLOUD_URL_PATTERN, url)
    if not m:
        url = pathlib.Path(url)
        return InLocalFileParam(url) if is_input else OutLocalFileParam(url)

    hostname, filepath = m['hostname'], m['abspath']

    return InCloudFileParam(filepath, hostname) if is_input else \
        OutCloudFileParam(filepath, hostname)
