import dataclasses
import pathlib
from typing import Dict, List, Optional, Tuple, Union

from cmdproxy import ipath, opath
from cmdproxy.invoke_params import InLocalFileParam, OutFileParam, \
    OutLocalFileParam, P, \
    ParamBase, StrParam


@dataclasses.dataclass
class FakeClientRunSpec:
    command: Union[str, P]
    args: List[Union[str, P]]
    stdout: Optional[Union[str, P]]
    stderr: Optional[Union[str, P]]
    env: Optional[Dict[str, Union[str, P]]]
    cwd: Optional[Union[str, P]]


@dataclasses.dataclass
class FakeClientRunCtx:
    spec: FakeClientRunSpec
    ret_code: int
    inputs: List[Tuple[pathlib.Path, InLocalFileParam]]
    outputs: Dict[pathlib.Path, Tuple[bytes, OutLocalFileParam]]


@dataclasses.dataclass
class FakeServerRunSpec:
    command: ParamBase
    args: List[ParamBase]
    stdout: Optional[ParamBase]
    stderr: Optional[ParamBase]
    env: Optional[Dict[str, Union[str, P]]]
    cwd: Optional[Union[str, P]]


@dataclasses.dataclass
class FakeServerRunCtx:
    spec: FakeServerRunSpec
    ret_code: int
    inputs: Dict[str, bytes]
    outputs: Dict[str, Tuple[bytes, OutFileParam]]
    local_input_files: List[pathlib.Path]
    local_output_files: List[pathlib.Path]


def create_fake_client_run_content(faker, fake_local_path_maker,
                                   fake_local_file_maker):
    inputs: List[Tuple[pathlib.Path, InLocalFileParam]] = []
    outputs: Dict[pathlib.Path, Tuple[bytes, OutLocalFileParam]] = {}

    def make_output_local_path():
        """Create a local path, and assign it with fake content."""
        _path = fake_local_path_maker()
        _param = opath(_path)
        outputs[_path] = (faker.text().encode(), _param)
        return _param

    def make_input_local_file():
        """Create a local path pointing to a prepared file."""
        _path: pathlib.Path = fake_local_file_maker()
        _param: InLocalFileParam = ipath(_path)
        inputs.append((_path, _param))
        return _param

    # the args that client may receive
    spec = FakeClientRunSpec(
        command='bin/bash',
        args=[
            '--flag=on',
            '--arg=value',
            make_input_local_file(),
            make_output_local_path(),
        ],
        stdout=make_output_local_path(),
        stderr=make_output_local_path(),
        env={
            'script': make_input_local_file()
        },
        cwd=None,
    )

    return FakeClientRunCtx(spec, 0, inputs, outputs)


def create_fake_server_run_content(faker, fake_local_path_maker,
                                   fake_cloud_file_maker, fs):
    inputs: Dict[str, bytes] = {}
    outputs: Dict[str, Tuple[bytes, OutFileParam]] = {}

    def make_input_cloud_file():
        path = fake_local_path_maker()
        param = ipath(path).as_cloud()
        content = fake_cloud_file_maker(fs, filename=param.cloud_url)
        inputs[param.cloud_url] = content
        return param

    def make_output_cloud_file():
        path = fake_local_path_maker()
        param = opath(path).as_cloud()
        content = faker.text().encode()
        outputs[param.cloud_url] = (content, param)
        return param

    # the args that a server may receive
    spec = FakeServerRunSpec(
        command=StrParam('/bin/bash'),
        args=[
            StrParam('--flag=on'),
            StrParam('--arg=value'),
            make_input_cloud_file(),
            make_output_cloud_file(),
        ],
        stdout=make_output_cloud_file(),
        stderr=make_output_cloud_file(),
        env=None,
        cwd=None,
    )

    return FakeServerRunCtx(spec, 0, inputs, outputs, [], [])
