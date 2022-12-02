import pathlib
from typing import Any, Dict, List, Tuple

from cmdproxy import ipath, opath
from cmdproxy.command_tool import CommandTool
from cmdproxy.invoke_middle import ProxyClientEndInvokeMiddle, \
    ProxyServerEndInvokeMiddle
from cmdproxy.invoke_params import InFileParam, InLocalFileParam, OutFileParam, \
    OutLocalFileParam, \
    ParamBase, StrParam


class TestProxyClientEnd:
    def test_correctly_maintain_files(self, grid_fs_maker, faker,
                                      fake_local_file_maker,
                                      fake_local_path_maker):
        fs = grid_fs_maker('test_client_correctly_maintain_files_db')

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
        _cwd = None
        _mock_ret = 0

        class MockTool(CommandTool):
            def __call__(self, *args, stdout, stderr=None, env=None, cwd=None):
                for origin_arg, arg in zip(
                        (*_args, _stdout, _stderr, *(_env or dict()).values()),
                        (*args, stdout, stderr, *(env or dict()).values())):
                    # assert all args are params,
                    assert isinstance(arg, ParamBase) if arg is not None \
                        else True

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

                return _mock_ret

        im = ProxyClientEndInvokeMiddle(fs)
        tool = im.wrap(tool=MockTool('/bin/bash'))

        ret = tool(*_args,
                   stdout=_stdout,
                   stderr=_stderr,
                   env=_env,
                   cwd=_cwd)
        assert ret == _mock_ret

        # assert all outputs are downloaded
        for output_path, (output_content, _) in outputs.items():
            assert output_path.exists()
            assert output_path.read_bytes() == output_content

        # assert all cloud inputs uploaded by client have been swept
        for _, input_param in inputs:
            assert not input_param.exists_on_cloud(fs)

        # assert all cloud outputs uploaded by server have been swept
        for _, output_param in outputs.values():
            assert not output_param.exists_on_cloud(fs)


class TestProxyServerEnd:
    def test_correctly_maintain_files(self, faker, grid_fs_maker,
                                      fake_cloud_file_maker,
                                      fake_local_path_maker):
        fs = grid_fs_maker('test_server_correctly_maintain_files_db')

        inputs: Dict[str, bytes] = {}
        outputs: Dict[str, Tuple[bytes, OutFileParam]] = {}

        local_input_files: List[pathlib.Path] = []
        local_output_files: List[pathlib.Path] = []

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
        _args = [
            StrParam('--flag=on'),
            StrParam('--arg=value'),
            make_input_cloud_file(),
            make_output_cloud_file(),
        ]
        _stdout = make_output_cloud_file()
        _stderr = make_output_cloud_file()
        _env: Dict[str, Any] or None = None
        _cwd = None
        _mock_ret = 0

        class MockTool(CommandTool):
            def __call__(self, *args, stdout, stderr=None, env=None, cwd=None):
                for origin_arg, arg in zip(
                        (*_args, _stdout, _stderr, *(_env or dict()).values()),
                        (*args, stdout, stderr, *(env or dict()).values())):
                    # assert all StrParams have been as strings
                    if isinstance(origin_arg, StrParam):
                        assert origin_arg.value == arg

                    # assert all inputs are downloaded
                    if isinstance(origin_arg, InFileParam):
                        path = pathlib.Path(arg)
                        local_input_files.append(path)
                        assert path.exists()

                        local_content = path.read_bytes()
                        cloud_content = inputs[origin_arg.cloud_url]
                        assert local_content == cloud_content

                    # imitate server end to write outputs
                    if isinstance(origin_arg, OutFileParam):
                        path = pathlib.Path(arg)
                        local_output_files.append(path)
                        content = outputs[origin_arg.cloud_url][0]
                        path.write_bytes(content)

                return _mock_ret

        im = ProxyServerEndInvokeMiddle(fs)
        tool = im.wrap(tool=MockTool('/bin/bash'))

        ret = tool(*_args,
                   stdout=_stdout,
                   stderr=_stderr,
                   env=_env,
                   cwd=_cwd)
        assert ret == _mock_ret

        # assert all outputs are uploaded
        for output_content, out_param in outputs.values():
            uploaded_content = out_param.download(fs)[1]
            assert output_content == uploaded_content

        # assert all local inputs downloaded by server have been swept
        for input_path in local_input_files:
            assert not input_path.exists()

        # assert all local outputs generated by process have been swept
        for output_path in local_output_files:
            assert not output_path.exists()
