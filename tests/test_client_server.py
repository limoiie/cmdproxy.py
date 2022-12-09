from collections import deque
from typing import cast

import celery
from autodict import Options
from autoserde import AutoSerde

from cmdproxy.client import Client
from cmdproxy.invoke_params import FileParamBase, FormatParam, StrParam
from cmdproxy.protocol import RunRequest, RunResponse
from cmdproxy.server import Server
from fake_run_context import create_fake_client_run_content, \
    create_fake_server_run_content


def test_client(redis, mongo, celery_session_app, celery_session_worker,
                faker, fake_local_path_maker, fake_local_file_maker,
                cmdproxy_client_config):
    """
    Test the client with a mock server. Assertions will be fired when the mock
    server receives the request, and when the client receives the return from
    the mock server end. During testing, the celery app and worker should be
    online.
    """
    ctx = create_fake_client_run_content(faker, fake_local_path_maker,
                                         fake_local_file_maker)

    @celery_session_app.task
    def mock_server_end(serialized_request: str):
        run_request = AutoSerde.deserialize(body=serialized_request,
                                            cls=RunRequest, fmt='json',
                                            options=Options(with_cls=False))
        stack = deque(zip(
            [*ctx.spec.args, ctx.spec.stdout, ctx.spec.stderr,
             *ctx.spec.env.values()],
            [*run_request.args, run_request.stdout, run_request.stderr,
             *run_request.env.values()]))

        # assert all the params has been transformed into expected style
        while stack:
            origin_arg, arg = stack.popleft()

            # also check all sub-params of FormatParam
            if isinstance(origin_arg, FormatParam):
                assert isinstance(arg, FormatParam)
                stack.extend(zip(origin_arg.args.values(), arg.args.values()))

            # assert all strings has become as StrParam
            if isinstance(origin_arg, str):
                assert arg == StrParam(origin_arg)

            # assert all file param has become CloudFileParm
            if isinstance(origin_arg, FileParamBase):
                assert arg == origin_arg.as_cloud()

        run_response = RunResponse(ctx.ret_code, None)
        return AutoSerde.serialize(run_response, fmt='json',
                                   options=Options(with_cls=False))

    celery_session_worker.reload()

    client = Client(cmdproxy_client_config, cast(celery.Task, mock_server_end))
    ret = client.run(
        command=ctx.spec.command,
        args=ctx.spec.args,
        stdout=ctx.spec.stdout,
        stderr=ctx.spec.stderr,
        env=ctx.spec.env,
        cwd=ctx.spec.cwd
    )

    assert ctx.ret_code == ret


def test_server(redis, mongo, faker, fake_cloud_file_maker,
                fake_local_path_maker, cmdproxy_server_config):
    """
    Test the server alone with a serialized request. The assertions will be
    fired when the server is ready to return. Since no client involved, the
    celery is offline during testing.
    """
    fs = cmdproxy_server_config.cloud.grid_fs()
    ctx = create_fake_server_run_content(faker, fake_local_path_maker,
                                         fake_cloud_file_maker, fs)
    run_request = RunRequest(
        command=ctx.spec.command,
        args=ctx.spec.args,
        stdout=ctx.spec.stdout,
        stderr=ctx.spec.stderr,
        env=ctx.spec.env,
        cwd=ctx.spec.cwd,
    )
    serialized = AutoSerde.serialize(run_request, fmt='json',
                                     options=Options(with_cls=False))

    server = Server(cmdproxy_server_config)
    serialized_response = server.run(serialized)

    run_response = AutoSerde.deserialize(body=serialized_response,
                                         cls=RunResponse, fmt='json',
                                         options=Options(with_cls=False))
    assert ctx.ret_code == run_response.return_code

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


def test_integrated(redis, mongo, celery_session_app, celery_session_worker,
                    faker, fake_local_path_maker, fake_local_file_maker,
                    cmdproxy_client_config):
    """
    Integration test. The client will send a well-made real-world request to the
    server, and the server will process it, and return the result. During the
    testing, celery should be online. Assertion will be made after client
    received the result returned by server.
    """
    fs = cmdproxy_client_config.cloud.grid_fs()
    ctx = create_fake_client_run_content(faker, fake_local_path_maker,
                                         fake_local_file_maker)

    client = Client.instance()
    ret = client.run(
        command=ctx.spec.command,
        args=ctx.spec.args,
        stdout=ctx.spec.stdout,
        stderr=ctx.spec.stderr,
        env=ctx.spec.env,
        cwd=ctx.spec.cwd
    )

    assert ctx.ret_code == ret

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
