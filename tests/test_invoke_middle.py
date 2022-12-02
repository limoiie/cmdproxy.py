import pathlib
from typing import Dict, List, Tuple

from cmdproxy import ipath, opath
from cmdproxy.command_tool import CommandTool
from cmdproxy.invoke_middle import ProxyClientEndInvokeMiddle
from cmdproxy.invoke_params import InLocalFileParam, OutLocalFileParam, \
    ParamBase


class TestProxyClientEnd:
    def test_wrap_args(self, grid_fs_maker, fake_local_file_maker,
                       fake_local_path_maker, faker):
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
                # assert all args are params,
                for arg in args:
                    assert isinstance(arg, ParamBase)

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

        tool(
            *_args,
            stdout=_stdout,
            stderr=_stderr,
            env=_env,
        )

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
