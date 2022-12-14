import dataclasses
import pathlib
from typing import Dict, List, Optional, Tuple, Union

from cmdproxy.invoke_params import InLocalFileParam, OutFileParam, \
    OutLocalFileParam, Param


@dataclasses.dataclass
class FakeClientRunSpec:
    command: Union[str, Param]
    args: List[Union[str, Param]]
    stdout: Optional[Union[str, Param]]
    stderr: Optional[Union[str, Param]]
    env: Optional[Dict[str, Union[str, Param]]]
    cwd: Optional[Union[str, Param]]


@dataclasses.dataclass
class FakeClientRunCtx:
    spec: FakeClientRunSpec
    ret_code: int
    inputs: List[Tuple[pathlib.Path, InLocalFileParam]]
    outputs: Dict[pathlib.Path, Tuple[bytes, OutLocalFileParam]]


@dataclasses.dataclass
class FakeServerRunSpec:
    command: Param
    args: List[Param]
    stdout: Optional[Param]
    stderr: Optional[Param]
    env: Optional[Dict[str, Union[str, Param]]]
    cwd: Optional[Union[str, Param]]


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

    def make_output_local_path(content=None):
        """Create a local path, and assign it with fake content."""
        _path = fake_local_path_maker()
        _param = Param.opath(_path)
        _content = faker.text().encode() if content is None else content
        outputs[_path] = (_content, _param)
        return _param

    def make_input_local_file(content=None):
        """Create a local path pointing to a prepared file."""
        _path: pathlib.Path = fake_local_file_maker(content)
        _param: InLocalFileParam = Param.ipath(_path)
        inputs.append((_path, _param))
        return _param

    some_content = faker.text().encode()

    # the args that client may receive
    spec = FakeClientRunSpec(
        command=Param.cmd_path('/bin/sh'),
        args=[
            '-c',
            Param.format(
                'cat {input} > {output}', {
                    'input': make_input_local_file(some_content),
                    'output': make_output_local_path(some_content),
                }
            ),
        ],
        stdout=make_output_local_path(b''),
        stderr=None,
        env={
            'script': make_input_local_file(),
            'string': Param.str('string value'),
            'int': 10,
            'float': 10.2,
            'bool': False,
        },
        cwd=None,
    )

    return FakeClientRunCtx(spec, 0, inputs, outputs)


def create_fake_server_run_content(faker, fake_local_path_maker,
                                   fake_cloud_file_maker, fs):
    inputs: Dict[str, bytes] = {}
    outputs: Dict[str, Tuple[bytes, OutFileParam]] = {}

    def make_input_cloud_file(content=None):
        path = fake_local_path_maker()
        param = Param.ipath(path).as_cloud()
        content = fake_cloud_file_maker(fs, param.cloud_url, content=content)
        inputs[param.cloud_url] = content
        return param

    def make_output_cloud_file(content=None):
        path = fake_local_path_maker()
        param = Param.opath(path).as_cloud()
        content = faker.text().encode() if content is None else content
        outputs[param.cloud_url] = (content, param)
        return param

    some_content = faker.text().encode()

    # the args that a server may receive
    spec = FakeServerRunSpec(
        command=Param.cmd_path('/bin/sh'),
        args=[
            Param.str('-c'),
            Param.format(
                'cat {input} > {output}', {
                    'input': make_input_cloud_file(some_content),
                    'output': make_output_cloud_file(some_content),
                }
            ),
        ],
        stdout=make_output_cloud_file(b''),
        stderr=None,
        env=None,
        cwd=None,
    )

    return FakeServerRunCtx(spec, 0, inputs, outputs, [], [])
