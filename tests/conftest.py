import contextlib
import dataclasses
import os
import pathlib
import tempfile
from typing import Any, Callable, List, Tuple

import gridfs
import pymongo
import pytest
from bson import ObjectId
from redis import Redis, from_url as redis_from_url

from cmdproxy.celery_app.config import init_client_conf, init_server_conf


@pytest.fixture(scope='session')
def redis() -> Redis:
    url = os.getenv('TEST_CMDPROXY_REDIS', default=None)
    if url:
        return redis_from_url(url)

    else:
        import testcontainers.redis

        with testcontainers.redis.RedisContainer() as container:
            yield container.get_client()


@pytest.fixture(scope='session')
def mongo() -> pymongo.MongoClient:
    url = os.getenv('TEST_CMDPROXY_MONGO', default=None)
    if url:
        return pymongo.MongoClient(url)

    else:
        import testcontainers.mongodb

        with testcontainers.mongodb.MongoDbContainer() as container:
            yield container.get_connection_client()


@pytest.fixture(scope='session')
def redis_url(redis):
    return uri_of_redis(redis)


@pytest.fixture(scope='session')
def mongo_url(mongo):
    return uri_of_mongo(mongo)


def uri_of_redis(r: Redis) -> str:
    conn = r.connection_pool.get_connection('echo')
    return 'redis://%s:%d' % (conn.host, conn.port)


def uri_of_mongo(m: pymongo.MongoClient) -> str:
    # noinspection PyProtectedMember
    kwargs = m._MongoClient__init_kwargs
    host, port = kwargs['host'], kwargs['port']
    port = port or 27017

    host = host.rsplit('/', maxsplit=1)[-1]
    if ':' in host:
        _host, _port = host.rsplit(':', maxsplit=1)

        with contextlib.suppress(ValueError):
            port = int(_port)
            host = _host

    return 'mongodb://%s:%d' % (host, port)


@pytest.fixture(scope='session')
def grid_fs_maker(mongo) -> Callable[[str], gridfs.GridFS]:
    def make_grid_fs(database_name):
        return gridfs.GridFS(mongo.get_database(database_name))

    yield make_grid_fs


@pytest.fixture(scope='session')
def resource():
    resource_root = pathlib.Path(__file__).parent

    def find(relpath):
        return (resource_root / relpath).resolve()

    return find


@pytest.fixture(scope='function')
def fake_local_file_maker(tmp_path, faker):
    paths = []

    def make(content=None, **kwargs):
        _path = pathlib.Path(tempfile.mktemp(dir=tmp_path, **kwargs))
        _path.write_bytes(faker.text().encode() if content is None else content)
        paths.append(_path)
        return _path

    try:
        yield make

    finally:
        for path in paths:
            if os.path.exists(path):
                os.remove(path)


@pytest.fixture(scope='function')
def fake_local_file(tmp_path, faker):
    """
    Path to a fake local file with random content.

    The fake file will be removed after this returned.
    """
    path = pathlib.Path(tempfile.mktemp(dir=tmp_path))
    path.write_text(faker.text())
    try:
        yield path

    finally:
        if os.path.exists(path):
            os.remove(path)


@pytest.fixture(scope='function')
def fake_local_path_maker(tmp_path):
    paths = []

    def make(**kwargs):
        _path = pathlib.Path(tempfile.mktemp(dir=tmp_path, **kwargs))
        paths.append(_path)
        return _path

    try:
        yield make

    finally:
        for path in paths:
            if os.path.exists(path):
                os.remove(path)


@pytest.fixture(scope='function')
def fake_cloud_file_maker(tmp_path, faker):
    files: List[Tuple[gridfs.GridFS, ObjectId]] = []

    def maker(fs: gridfs.GridFS, filename: str, content=None):
        _content = faker.text().encode() if content is None else content
        _file_id = fs.put(_content, filename=filename)
        files.append((fs, _file_id))
        return _content

    try:
        yield maker

    finally:
        for fs_, file_id in files:
            fs_.delete(file_id)


@dataclasses.dataclass
class Raises:
    exc: Any or Tuple[Any, ...]
    kwargs: dict = dataclasses.field(default_factory=dict)


def case_name(case):
    return case.name


@pytest.fixture(scope='session')
def cmdproxy_server_config(redis_url, mongo_url, resource):
    conf = init_server_conf(redis_url=redis_url,
                            mongo_url=mongo_url,
                            mongodb_name='test-cmdproxy',
                            command_palette=resource(
                                'command-palette.yaml'))
    yield conf


@pytest.fixture(scope='session')
def cmdproxy_client_config(redis_url, mongo_url):
    conf = init_client_conf(redis_url=redis_url,
                            mongo_url=mongo_url,
                            mongodb_name='test-cmdproxy')
    yield conf


@pytest.fixture(scope='session')
def celery_config(cmdproxy_client_config, cmdproxy_server_config):
    broker_url = cmdproxy_client_config.celery.broker_url
    backend_url = cmdproxy_client_config.celery.backend_url

    print(f'running celery with broker {broker_url}, backend: {backend_url}')
    return {
        'broker_url': broker_url,
        'result_backend': backend_url,
    }


@pytest.fixture(scope='session')
def celery_worker_parameters():
    return {
        'queues': ('sh', 'celery'),
    }


@pytest.fixture(scope='session')
def celery_includes(cmdproxy_client_config, cmdproxy_server_config):
    return [
        'cmdproxy.celery_app.tasks',
    ]
