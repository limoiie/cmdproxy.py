import dataclasses
import os
import pathlib
import tempfile
from typing import Any, Callable, Tuple

import gridfs
import pymongo
import pytest
import redis


@pytest.fixture(scope='session')
def redis() -> redis.Redis:
    if os.getenv('TESTENV_READY'):
        # todo: get redis conf from .github/workflow/...
        pass

    else:
        import testcontainers.redis

        with testcontainers.redis.RedisContainer() as container:
            yield container.get_client()


@pytest.fixture(scope='session')
def mongo() -> pymongo.MongoClient:
    if os.getenv('TESTENV_READY'):
        # todo: get redis conf from .github/workflow/...
        pass

    else:
        import testcontainers.mongodb

        with testcontainers.mongodb.MongoDbContainer() as container:
            yield container.get_connection_client()


@pytest.fixture(scope='session')
def grid_fs_maker(mongo) -> Callable[[str], gridfs.GridFS]:
    def make_grid_fs(database_name):
        return gridfs.GridFS(mongo.get_database(database_name))

    yield make_grid_fs


@pytest.fixture(scope='function')
def fake_local_file_maker(tmp_path, faker):
    paths = []

    def make(**kwargs):
        _path = pathlib.Path(tempfile.mktemp(dir=tmp_path, **kwargs))
        _path.write_text(faker.text())
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
def fake_cloud_file_maker(tmp_path, faker, grid_fs_maker):
    file_id_list = []

    def maker(database_name, filename):
        _fs = grid_fs_maker(database_name)
        _content = faker.text().encode()
        _file_id = _fs.put(_content, filename=filename)
        file_id_list.append((_fs, _file_id))
        return _fs, _content

    try:
        yield maker

    finally:
        for fs, file_id in file_id_list:
            fs: gridfs.GridFS
            fs.delete(file_id)


@dataclasses.dataclass
class Raises:
    exc: Any or Tuple[Any, ...]
    kwargs: dict = dataclasses.field(default_factory=dict)


def case_name(case):
    return case.name
