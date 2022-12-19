from autodict import Options
from autoserde import AutoSerde

from cmdproxy.invoke_params import Param
from cmdproxy.protocol import RunRequest


def test_serde(faker, fake_local_path_maker):
    req = RunRequest(
        command=Param.str(faker.name()),
        args=(
            Param.str(faker.name()),
            Param.str(faker.name()),
            Param.ipath(fake_local_path_maker()).as_cloud(),
            Param.opath(fake_local_path_maker()).as_cloud(),
            Param.format('--path={path}', {
                'path': Param.ipath(fake_local_path_maker()).as_cloud(),
            }),
        ),
        cwd=None,
        env={
            faker.name(): Param.ipath(fake_local_path_maker()).as_cloud(),
            faker.name(): Param.opath(fake_local_path_maker()).as_cloud(),
        },
        stdout=Param.opath(fake_local_path_maker()).as_cloud(),
        stderr=Param.opath(fake_local_path_maker()).as_cloud(),
    )
    options = Options(with_cls=False)

    serialized = AutoSerde.serialize(req, fmt='json', options=options)
    deserialized_req = AutoSerde.deserialize(
        body=serialized, cls=RunRequest, fmt='json', options=options)

    assert req == deserialized_req
