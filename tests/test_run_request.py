from autodict import Options
from autoserde import AutoSerde

from cmdproxy.invoke_params import FormatParam, StrParam, ipath, opath
from cmdproxy.run_request import RunRequest


def test_serde(faker, fake_local_path_maker):
    req = RunRequest(
        command=StrParam(faker.name()),
        args=(
            StrParam(faker.name()),
            StrParam(faker.name()),
            ipath(fake_local_path_maker()).as_cloud(),
            opath(fake_local_path_maker()).as_cloud(),
            FormatParam('--path={}', (
                ipath(fake_local_path_maker()).as_cloud(),
            )),
        ),
        cwd=None,
        env={
            faker.name(): ipath(fake_local_path_maker()).as_cloud(),
            faker.name(): opath(fake_local_path_maker()).as_cloud(),
        },
        stdout=opath(fake_local_path_maker()).as_cloud(),
        stderr=opath(fake_local_path_maker()).as_cloud(),
    )
    options = Options(with_cls=False)

    serialized = AutoSerde.serialize(req, fmt='json', options=options)
    deserialized_req = AutoSerde.deserialize(
        body=serialized, cls=RunRequest, fmt='json', options=options)

    assert req == deserialized_req
