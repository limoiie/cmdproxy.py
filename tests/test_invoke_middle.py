import dataclasses
import io
import pathlib
from collections import deque

import parse

from cmdproxy.invoke_params import FormatParam, InCloudFileParam, InFileParam, \
    OutCloudFileParam, OutFileParam, Param, StrParam
from cmdproxy.middles import ProxyClientEndInvokeMiddle, \
    ProxyServerEndInvokeMiddle
from fake_run_context import create_fake_client_run_content, \
    create_fake_server_run_content


class TestProxyClientEnd:
    def test_in_stream_guard(self, faker, grid_fs_maker):
        fs = grid_fs_maker('test_in_stream_guard_db')
        # fake the source bytes for in stream
        content: bytes = faker.binary(length=200)

        # make up a testing middle
        im = ProxyClientEndInvokeMiddle(fs)
        guard = ProxyClientEndInvokeMiddle.InStreamGuard(ctx=im)

        # create the target in stream
        stream = io.BytesIO(content)
        param = Param.istream(io=stream, filename=faker.file_path())

        with guard.guard(param, None) as f_param:
            assert isinstance(f_param, InCloudFileParam), \
                'StreamParam should be guarded as a CloudFileParam'
            assert f_param.exists_on_cloud(fs), \
                'StreamParam should be uploaded after being guarded'

            uploaded_content = f_param.download(fs)[1]
            assert content == uploaded_content, 'inconsistent uploaded content'

        assert not f_param.exists_on_cloud(fs), \
            'uploaded file should be removed from cloud after all'

    def test_out_stream_guard(self, faker, grid_fs_maker):
        fs = grid_fs_maker('test_out_stream_guard_db')
        content: bytes = faker.binary(length=200)

        im = ProxyClientEndInvokeMiddle(fs)
        guard = ProxyClientEndInvokeMiddle.OutStreamGuard(ctx=im)

        stream = io.BytesIO(content)
        param = Param.ostream(io=stream, filename=faker.file_path())

        with guard.guard(param, None) as f_param:
            assert isinstance(f_param, OutCloudFileParam), \
                'StreamParam should be guarded as a CloudFileParam'

            # mimic server-end uploading
            f_param.upload(fs, body=content)

        uploaded_content = stream.read()
        assert content == uploaded_content, 'inconsistent uploaded content'

        assert not f_param.exists_on_cloud(fs), \
            'uploaded file should be removed from cloud after all'

    def test_correctly_maintain_files(self, grid_fs_maker, faker,
                                      fake_local_file_maker,
                                      fake_local_path_maker):
        fs = grid_fs_maker('test_client_correctly_maintain_files_db')
        ctx = create_fake_client_run_content(faker, fake_local_path_maker,
                                             fake_local_file_maker)

        @dataclasses.dataclass
        class MockTool:
            command: str

            def __call__(self, *args, stdout, stderr=None, env=None, cwd=None):
                stack = deque(zip(
                    (*ctx.spec.args, ctx.spec.stdout, ctx.spec.stderr,
                     *(ctx.spec.env or dict()).values()),
                    (*args, stdout, stderr, *(env or dict()).values())))

                while stack:
                    origin_arg, arg = stack.popleft()

                    # also check all sub-params of FormatParam
                    if isinstance(origin_arg, FormatParam):
                        assert isinstance(arg, FormatParam)
                        stack.extend(
                            zip(origin_arg.args.values(), arg.args.values()))

                    # assert all args are params,
                    assert isinstance(arg, Param) if arg is not None else True

                    # assert all the non-param args have been as StrParam
                    if not isinstance(origin_arg, Param):
                        assert isinstance(arg, StrParam)

                # assert all inputs are uploaded
                for _path, _param in ctx.inputs:
                    cloud_content = _param.download(fs)[1]
                    local_content = _path.read_bytes()
                    assert cloud_content == local_content

                # imitate server end to upload the outputs
                for _content, _param in ctx.outputs.values():
                    _param.upload(fs, body=_content)

                return ctx.ret_code

        im = ProxyClientEndInvokeMiddle(fs)
        tool = im.wrap(func=MockTool(ctx.spec.command))

        ret = tool(*ctx.spec.args,
                   stdout=ctx.spec.stdout,
                   stderr=ctx.spec.stderr,
                   env=ctx.spec.env,
                   cwd=ctx.spec.cwd)
        assert ret == ctx.ret_code

        # assert all outputs are downloaded
        for output_path, (output_content, _) in ctx.outputs.items():
            assert output_path.exists()
            assert output_path.read_bytes() == output_content

        # assert all cloud inputs uploaded by client have been swept
        for _, input_param in ctx.inputs:
            assert not input_param.exists_on_cloud(fs)

        # assert all cloud outputs uploaded by server have been swept
        for _, output_param in ctx.outputs.values():
            assert not output_param.exists_on_cloud(fs)


class TestProxyServerEnd:
    def test_correctly_maintain_files(self, faker, grid_fs_maker,
                                      fake_cloud_file_maker,
                                      fake_local_path_maker):
        fs = grid_fs_maker('test_server_correctly_maintain_files_db')
        ctx = create_fake_server_run_content(faker, fake_local_path_maker,
                                             fake_cloud_file_maker, fs)

        @dataclasses.dataclass
        class MockTool:
            command: str

            def __call__(self, *args, stdout, stderr=None, env=None, cwd=None):
                stack = deque(zip(
                    (*ctx.spec.args, ctx.spec.stdout, ctx.spec.stderr,
                     *(ctx.spec.env or dict()).values()),
                    (*args, stdout, stderr, *(env or dict()).values())))
                while stack:
                    origin_arg, arg = stack.popleft()

                    # also check all sub-params of FormatParam
                    if isinstance(origin_arg, FormatParam):
                        assert isinstance(arg, str)
                        args_ = parse.parse(origin_arg.tmpl, arg).named
                        stack.extend(
                            zip(origin_arg.args.values(), args_.values()))

                    # assert all StrParams have been as strings
                    if isinstance(origin_arg, StrParam):
                        assert origin_arg.value == arg

                    # assert all inputs are downloaded
                    if isinstance(origin_arg, InFileParam):
                        path = pathlib.Path(arg)
                        ctx.local_input_files.append(path)
                        assert path.exists()

                        local_content = path.read_bytes()
                        cloud_content = ctx.inputs[origin_arg.cloud_url]
                        assert local_content == cloud_content

                    # imitate server end to write outputs
                    if isinstance(origin_arg, OutFileParam):
                        path = pathlib.Path(arg)
                        ctx.local_output_files.append(path)
                        content = ctx.outputs[origin_arg.cloud_url][0]
                        path.write_bytes(content)

                return ctx.ret_code

        im = ProxyServerEndInvokeMiddle(fs)
        tool = im.wrap(func=MockTool(ctx.spec.command))

        ret = tool(*ctx.spec.args,
                   stdout=ctx.spec.stdout,
                   stderr=ctx.spec.stderr,
                   env=ctx.spec.env,
                   cwd=ctx.spec.cwd)
        assert ret == ctx.ret_code

        # assert all outputs are uploaded
        for output_content, out_param in ctx.outputs.values():
            uploaded_content = out_param.download(fs)[1]
            assert output_content == uploaded_content

        # assert all local inputs downloaded by server have been swept
        for input_path in ctx.local_input_files:
            assert not input_path.exists()

        # assert all local outputs generated by process have been swept
        for output_path in ctx.local_output_files:
            assert not output_path.exists()
