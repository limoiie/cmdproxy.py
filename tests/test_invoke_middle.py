import pathlib
from typing import Dict, List, Tuple

from cmdproxy import ipath, opath
from cmdproxy.command_tool import CommandTool
from cmdproxy.invoke_middle import ProxyClientEndInvokeMiddle
from cmdproxy.invoke_params import InLocalFileParam, OutLocalFileParam, \
    ParamBase, StrParam


class TestProxyClientEnd:
    def test_correctly_maintain_files(self, grid_fs_maker, faker,
                                      fake_local_file_maker,
                                      fake_local_path_maker):
        fs = grid_fs_maker('test_wrap_args_db')

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

        _args = [
            '--flag=on',
            '--arg=value',
            make_input_local_file(),
            make_output_local_path(),
        ]
        _stdout = make_output_local_path()
        _stderr = make_output_local_path()
        _env = {
            'script': make_input_local_file()
        }

        class MockTool(CommandTool):
            def __call__(self, *args, stdout, stderr=None, env=None, cwd=None):
                for origin_arg, arg in zip(_args, args):
                    # assert all args are params,
                    assert isinstance(arg, ParamBase)

                    # assert all the non-param args have been as StrParam
                    if not isinstance(origin_arg, ParamBase):
                        assert isinstance(arg, StrParam)

                # assert all inputs are uploaded
                for _path, _param in inputs:
                    cloud_content = _param.download(fs)[1]
                    local_content = _path.read_bytes()
                    assert cloud_content == local_content

                # imitate server end to upload the outputs
                for _content, _param in outputs.values():
                    _param.upload(fs, body=_content)

                return 0

        im = ProxyClientEndInvokeMiddle(fs)
        tool = im.wrap(tool=MockTool('/bin/bash'))

        ret = tool(*_args,
                   stdout=_stdout,
                   stderr=_stderr,
                   env=_env)
        assert ret == 0

        # assert all outputs are downloaded
        for output_path, (output_content, _) in outputs.items():
            assert output_path.exists()
            assert output_path.read_bytes() == output_content

        # assert all inputs uploaded by client have been swept
        for _, input_param in inputs:
            assert not input_param.exists_on_cloud(fs)

        # assert all outputs uploaded by server have been swept
        for _, output_param in outputs.values():
            assert not output_param.exists_on_cloud(fs)


class TestProxyServerEnd:
    pass
