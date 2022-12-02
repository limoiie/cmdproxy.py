import contextlib
import os
import pathlib
import socket
from collections import namedtuple
from os.path import basename
from pathlib import Path

import flexio
import pytest

from cmdproxy.invoke_params import InCloudFileParam, \
    InLocalFileParam, OutCloudFileParam, OutLocalFileParam, ipath, opath
from tests.conftest import case_name

MMeta = namedtuple('MetaDataMake', 'absolute,is_cloud,is_in,name')
MCase = namedtuple('CaseDataMake',
                   'ref,hostname,fpath,url,is_cloud,is_in,expected,name')


@pytest.fixture(scope='function')
def make_case(request, faker) -> MCase:
    meta: MMeta = request.param

    hostname = socket.gethostname()
    fpath = faker.file_path(absolute=meta.absolute)
    url = '@%s:%s' % (hostname, fpath)

    if meta.is_cloud:
        if meta.is_in:
            cls = InCloudFileParam
        else:
            cls = OutCloudFileParam
    else:
        if meta.is_in:
            cls = InLocalFileParam
        else:
            cls = OutLocalFileParam

    if meta.is_cloud:
        ref = url
        expected = cls(fpath, hostname)
    else:
        ref = fpath
        expected = cls(fpath)

    return MCase(
        ref=ref,
        hostname=hostname,
        fpath=fpath,
        url=url,
        is_cloud=meta.is_cloud,
        is_in=meta.is_in,
        expected=expected,
        name=meta.name,
    )


class TestFileParamConstruction:
    cases = [
        MMeta(name='abs path local in',
              absolute=True, is_cloud=False, is_in=True),
        MMeta(name='rel path local in',
              absolute=False, is_cloud=False, is_in=True),

        MMeta(name='abs path local out',
              absolute=True, is_cloud=False, is_in=False),
        MMeta(name='rel path local out',
              absolute=False, is_cloud=False, is_in=False),

        MMeta(name='abs path cloud in',
              absolute=True, is_cloud=True, is_in=True),
        MMeta(name='rel path cloud in',
              absolute=False, is_cloud=True, is_in=True),

        MMeta(name='abs path cloud out',
              absolute=True, is_cloud=True, is_in=True),
        MMeta(name='rel path cloud out',
              absolute=False, is_cloud=True, is_in=True),
    ]

    @pytest.mark.parametrize('make_case', cases, indirect=True, ids=case_name)
    def test_basic(self, make_case: MCase):
        case = make_case
        param = ipath(case.ref) if case.is_in else opath(case.ref)

        assert param.filepath == Path(case.fpath)
        assert param.is_cloud() == case.is_cloud
        assert param.filename == basename(case.fpath)
        assert param.cloud_url == case.url
        assert param == case.expected

    @pytest.mark.parametrize('make_case', cases, indirect=True, ids=case_name)
    def test_to_cloud(self, make_case: MCase):
        case = make_case
        param = ipath(case.ref) if case.is_in else opath(case.ref)

        cloud_param = param.as_cloud()
        assert cloud_param.is_cloud()
        assert cloud_param.filename == basename(case.fpath)
        assert cloud_param.hostname == case.hostname
        assert cloud_param.cloud_url == case.url


UMeta = namedtuple('MetaDataUpload',
                   'upload_type,raises,name')
UCase = namedtuple('CaseDataUpload',
                   'fs,param,fp,body,content,raises,name')


@pytest.fixture(scope='function')
def upload_case(request, fake_local_file, grid_fs_maker):
    meta: UMeta = request.param

    fs = grid_fs_maker('test_upload_db')
    content = fake_local_file.read_bytes()
    param = ipath(fake_local_file)
    ctx = contextlib.nullcontext()

    if meta.upload_type == 'bytes':
        os.remove(fake_local_file)
        fp = None
        body = content

    elif meta.upload_type == 'path':
        fp = pathlib.Path(fake_local_file)
        body = None

    elif meta.upload_type == 'str path':
        fp = str(fake_local_file)
        body = None

    elif meta.upload_type == 'file':
        fp = open(fake_local_file, 'rb')
        body = None
        ctx = fp

    else:
        raise ValueError(f'invalid fp type: {meta.upload_type}')

    with ctx:
        yield UCase(name=meta.name, fs=fs, param=param, fp=fp, body=body,
                    content=content, raises=None)


DMeta = namedtuple('MetaDataUpload',
                   'download_type,raises,name')


@pytest.fixture(scope='function')
def download_case(request, fake_local_file, fake_cloud_file_maker):
    meta: DMeta = request.param

    param = ipath(fake_local_file)
    fs, content = fake_cloud_file_maker(
        database_name='test_download_db', filename=param.cloud_url)
    ctx = contextlib.nullcontext()

    os.remove(fake_local_file)

    if meta.download_type == 'bytes':
        fp = None

    elif meta.download_type == 'path':
        fp = pathlib.Path(fake_local_file)

    elif meta.download_type == 'str path':
        fp = str(fake_local_file)

    elif meta.download_type == 'file':
        fp = open(fake_local_file, 'wb+')
        ctx = fp

    else:
        raise ValueError(f'invalid fp type: {meta.download_type}')

    with ctx:
        yield UCase(name=meta.name, fs=fs, param=param, fp=fp, body=None,
                    content=content, raises=None)


class TestFileParamInteraction:
    cases = [
        UMeta(name='upload bytes',
              upload_type='bytes', raises=None),
        UMeta(name='upload path',
              upload_type='path', raises=None),
        UMeta(name='upload str path',
              upload_type='str path', raises=None),
        UMeta(name='upload file',
              upload_type='file', raises=None),
    ]

    @pytest.mark.parametrize('upload_case', cases, indirect=True, ids=case_name)
    def test_upload(self, upload_case: UCase, grid_fs_maker):
        case = upload_case

        file_id = case.param.upload(case.fs, case.fp, body=case.body)
        with case.fs.get(file_id) as cloud_file:
            cloud_content = cloud_file.read()

        assert case.content == cloud_content

    cases = [
        DMeta(name='download bytes',
              download_type='bytes', raises=None),
        DMeta(name='download path',
              download_type='path', raises=None),
        DMeta(name='download str path',
              download_type='str path', raises=None),
        DMeta(name='download file',
              download_type='file', raises=None),
    ]

    @pytest.mark.parametrize('download_case', cases, indirect=True,
                             ids=case_name)
    def test_download(self, download_case: UCase):
        case = download_case

        _file_id, body = case.param.download(case.fs, case.fp)
        with flexio.FlexBinaryIO(case.fp, init=body) as io:
            io.seek(0)
            download_content = io.read()

        assert case.content == download_content
