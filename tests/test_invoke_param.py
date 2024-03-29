import contextlib
import filecmp
import os
import pathlib
import random
import socket
import zipfile
from collections import namedtuple
from os.path import basename
from pathlib import Path
from typing import cast

import flexio
import pytest
from autodict import AutoDict, Options

from cmdproxy.invoke_params import InCloudFileParam, InLocalFileParam, \
    OutCloudFileParam, OutLocalFileParam, Param
from conftest import case_name

MMeta = namedtuple('MetaDataMake', 'absolute,is_cloud,is_in,name')
MCase = namedtuple('CaseDataMake',
                   'ref,hostname,fpath,url,is_cloud,is_in,expected,name')


@pytest.fixture(scope='function')
def make_case(request, faker) -> MCase:
    meta: MMeta = request.param

    hostname = socket.gethostname()
    fpath = faker.file_path(depth=random.randint(1, 4))
    if not meta.absolute:
        fpath = fpath[1:]
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
        param = Param.ipath(case.ref) if case.is_in else Param.opath(case.ref)

        assert param.filepath == Path(case.fpath)
        assert param.is_cloud() == case.is_cloud
        assert param.filename == basename(case.fpath)
        assert param.cloud_url == case.url
        assert param == case.expected

    @pytest.mark.parametrize('make_case', cases, indirect=True, ids=case_name)
    def test_to_cloud(self, make_case: MCase):
        case = make_case
        param = Param.ipath(case.ref) if case.is_in else Param.opath(case.ref)

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
    param = Param.ipath(fake_local_file)
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
def download_case(request, fake_local_file, fake_cloud_file_maker,
                  grid_fs_maker):
    meta: DMeta = request.param

    fs = grid_fs_maker('test_download_db')
    param = Param.ipath(fake_local_file)
    content = fake_cloud_file_maker(fs=fs, filename=param.cloud_url)
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
        with flexio.flex_open(case.fp, init=body) as io:
            io.seek(0)
            download_content = io.read()

        assert case.content == download_content

    def test_upload_directory(self, resources, grid_fs_maker, tmp_path,
                              fake_local_file_maker):
        to_download_path = tmp_path / 'unzip-downloaded'
        to_upload_path = resources('fake_folder')
        fs = grid_fs_maker('test_upload_directory')

        param = Param.ipath(to_upload_path)
        param.upload(fs, to_upload_path)

        with fs.find_one(dict(filename=param.cloud_url)) as f:
            assert f.metadata['content_type'] == 'application/directory+zip'
            # download the whole file manually, and compare
            download_zip_path = fake_local_file_maker(content=f.read())

        with zipfile.ZipFile(download_zip_path) as download_zip:
            download_zip.extractall(to_download_path)

        res = filecmp.dircmp(to_upload_path, to_download_path)
        assert not res.left_only, f'These are only left has {res.left_only}'
        assert not res.right_only, f'These are only right has {res.right_only}'
        assert not res.diff_files, f'These are diff {res.diff_files}'
        assert not res.funny_files, f'These are not compared {res.funny_files}'

    def test_download_directory(self, resources, grid_fs_maker, tmp_path):
        to_upload_path = resources('fake_folder')
        downloaded_path = tmp_path
        fs = grid_fs_maker('test_download_directory')

        param = Param.ipath(downloaded_path)
        param.upload(fs, to_upload_path)
        param.download(fs, downloaded_path)

        res = filecmp.dircmp(downloaded_path, to_upload_path)
        assert not res.left_only, f'These are only left has {res.left_only}'
        assert not res.right_only, f'These are only right has {res.right_only}'
        assert not res.diff_files, f'These are diff {res.diff_files}'
        assert not res.funny_files, f'These are not compared {res.funny_files}'


@pytest.fixture(scope='function')
def mp_case(request, fake_local_path_maker, faker):
    meta: TestParamSerde.Meta = request.param

    if meta.kind == 'file':
        param = Param.ipath(fake_local_path_maker()) if meta.conf['is_in'] else \
            Param.opath(fake_local_path_maker())
        param = param.as_cloud() if meta.conf['is_cloud'] else param
        obj = {
            AutoDict.meta_of(type(param)).name: {
                'filepath': str(param.filepath),
                'hostname': param.hostname,
            }
        }

    elif meta.kind == 'str':
        param = Param.str(faker.text())
        obj = {
            AutoDict.meta_of(type(param)).name: {
                'value': param.value,
            }
        }

    elif meta.kind == 'format':
        param = Param.format("cat {input} > {output}", {
            'input': Param.ipath(fake_local_path_maker()),
            'output': Param.opath(fake_local_path_maker()),
        })
        obj = {
            AutoDict.meta_of(type(param)).name: {
                'tmpl': param.tmpl,
                'args': dict(
                    (name, arg.to_dict(meta.opts))
                    for name, arg in param.args.items()
                )
            }
        }

    elif meta.kind == 'config':
        param = Param.env(faker.text())
        obj = {
            AutoDict.meta_of(type(param)).name: {
                'name': param.name
            }
        }

    else:
        raise ValueError(f'Unknown param type: {meta.kind}')

    if meta.opts.with_cls:
        obj[AutoDict.CLS_ANNO_KEY] = AutoDict.meta_of(type(param)).name

    return TestParamSerde.Case(
        name=meta.name,
        param=param,
        opts=meta.opts,
        obj=obj,
        raises=meta.raises,
    )


class TestParamSerde:
    Meta = namedtuple('MetaParam', 'kind,conf,opts,raises,name')
    Case = namedtuple('CaseParam', 'param,opts,obj,raises,name')

    cases = [
        Meta(name='local input file param',
             kind='file',
             conf=dict(is_in=True, is_cloud=False),
             opts=Options(with_cls=True),
             raises=None),
        Meta(name='local output file param',
             kind='file',
             conf=dict(is_in=False, is_cloud=False),
             opts=Options(with_cls=True),
             raises=None),
        Meta(name='cloud input file param',
             kind='file',
             conf=dict(is_in=True, is_cloud=True),
             opts=Options(with_cls=True),
             raises=None),
        Meta(name='cloud output file param',
             kind='file',
             conf=dict(is_in=False, is_cloud=True),
             opts=Options(with_cls=True),
             raises=None),
        Meta(name='str param',
             kind='str',
             conf=None,
             opts=Options(with_cls=True),
             raises=None),
        Meta(name='config param',
             kind='config',
             conf=None,
             opts=Options(with_cls=True),
             raises=None),
        Meta(name='format param',
             kind='format',
             conf=None,
             opts=Options(with_cls=True),
             raises=None),
        Meta(name='format param without class',
             kind='format',
             conf=None,
             opts=Options(with_cls=False),
             raises=None),
    ]

    @pytest.mark.parametrize('mp_case', cases, indirect=True, ids=case_name)
    def test_to_dict(self, mp_case: Case, fake_local_path_maker):
        case = mp_case
        param = cast(Param, case.param)

        if case.raises:
            with pytest.raises(case.raises.exc, **case.raises.kwargs):
                param.to_dict(options=case.opts)
            return

        assert param.to_dict(options=case.opts) == case.obj

    @pytest.mark.parametrize('mp_case', cases, indirect=True, ids=case_name)
    def test_from_dict(self, mp_case: Case, fake_local_path_maker):
        case = mp_case
        if case.raises:
            with pytest.raises(case.raises.exc, **case.raises.kwargs):
                Param.from_dict(case.obj, options=case.opts)
            return

        assert Param.from_dict(case.obj, options=case.opts) == case.param
