import dataclasses
import os
import pathlib
import re
from abc import ABC, abstractmethod
from pathlib import Path
from socket import gethostname
from typing import IO, Tuple

import flexio
from bson import ObjectId
from gridfs import GridFS, GridOut


class ParamBase:
    pass


@dataclasses.dataclass
class ConfigParam(ParamBase):
    param_key: str


@dataclasses.dataclass
class RemoteConfigParam(ConfigParam):
    pass


LINK_REGEX = re.compile(r'<#:([io])>(.+?)</>')
CLOUD_URL_REGEX = re.compile(r'@([^:]+):(.+)')
CLOUD_URL = '@{hostname}:{abspath}'


@dataclasses.dataclass
class FileParamBase(ParamBase):
    def __init__(self, filepath: str or Path, hostname: str):
        self._filepath = Path(filepath)
        self._hostname = hostname

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
        return CLOUD_URL.format(hostname=self.hostname,
                                abspath=self.filepath.as_posix())

    @property
    def filepath(self):
        return self._filepath

    @property
    def filename(self):
        return str(self._filepath.name)

    @property
    def hostname(self):
        return self._hostname

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

    def download(self, fs: GridFS,
                 fp: IO[bytes] or os.PathLike or str or None = None) \
            -> Tuple[ObjectId, bytes] or Tuple[ObjectId, None]:
        with flexio.FlexBinaryIO(fp, 'wb+') as tgt:
            with self.find_on_cloud(fs) as src:
                tgt.write(src.read())

            if tgt.in_mem:
                tgt.seek(0)
                # noinspection PyProtectedMember
                return src._id, tgt.read()

            # noinspection PyProtectedMember
            return src._id, None

    def upload(self, fs: GridFS,
               fp: IO[bytes] or os.PathLike or str or None = None,
               body: bytes or None = None) -> ObjectId:
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
    def __init__(self, filepath: str or Path, hostname: str):
        super(CloudFileParam, self).__init__(filepath, hostname)

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
    def __init__(self, filepath: str or Path):
        super(LocalFileParam, self).__init__(filepath, gethostname())

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
    def __init__(self, filepath: str or Path, hostname: str):
        super().__init__(filepath, hostname)

    def as_cloud(self) -> 'InCloudFileParam':
        return self


class InLocalFileParam(LocalFileParam, InFileParam):
    def __init__(self, filepath: str or Path):
        super().__init__(filepath)


class OutCloudFileParam(CloudFileParam, OutFileParam):
    def __init__(self, filepath: str or Path, hostname: str):
        super().__init__(filepath, hostname)

    def as_cloud(self) -> 'OutCloudFileParam':
        return self


class OutLocalFileParam(LocalFileParam, OutFileParam):
    def __init__(self, filepath: str or Path):
        super().__init__(filepath)


def ipath(ref: str or pathlib.Path) -> InLocalFileParam or InCloudFileParam:
    """
    Create either an InLocalFileParam or an InCloudFileParam according to url.

    :param ref: Either being a filepath (for local), or a cloud_url (for cloud).
    :return: A sub instance of InFileParam.
    """
    return _file(ref, is_input=True)


def opath(ref: str or pathlib.Path) -> OutLocalFileParam or OutCloudFileParam:
    """
    Create either an OutLocalFileParam or an OutCloudFileParam according to url.

    :param ref: Either being a filepath (for local), or a cloud_url (for cloud).
    :return: A sub instance of OutFileParam.
    """
    return _file(ref, is_input=False)


@dataclasses.dataclass
class FormatParam(ParamBase):
    tmpl: str
    args: list


@dataclasses.dataclass
class StrParam(ParamBase):
    value: str


def upload_as_in(fs: GridFS, path: str or Path) -> InCloudFileParam:
    param = ipath(path).as_cloud()
    param.upload_(fs)
    return param


def alloc_as_out(fs: GridFS, path: str or Path) -> OutCloudFileParam:
    param = opath(path).as_cloud()
    param.alloc_(fs)
    return param


def _file(url, is_input):
    if isinstance(url, Path):
        return InLocalFileParam(url) if is_input else OutLocalFileParam(url)

    m = CLOUD_URL_REGEX.match(url)
    if not m:
        return InLocalFileParam(url) if is_input else OutLocalFileParam(url)

    hostname, filepath = m.groups()

    return InCloudFileParam(filepath, hostname) if is_input else \
        OutCloudFileParam(filepath, hostname)
