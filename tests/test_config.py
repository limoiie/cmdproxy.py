import os

import pytest_mock

from cmdproxy import init_client_conf
from cmdproxy.celery_app.config import get_client_end_conf, get_server_end_conf, \
    init_server_conf


class TestClientConfig:
    def test_init_client_by_cli(self, faker):
        fake_redis_url = faker.url(['redis'])
        fake_mongo_url = faker.url(['mongodb'])
        fake_mongodb_name = faker.first_name()

        init_client_conf(redis_url=fake_redis_url, mongo_url=fake_mongo_url,
                         mongodb_name=fake_mongodb_name)

        conf = get_client_end_conf()

        assert conf.celery.broker_url == fake_redis_url
        assert conf.celery.backend_url == fake_mongo_url
        assert conf.cloud.mongodb_name == fake_mongodb_name

    def test_init_client_by_env(self, faker, mocker: pytest_mock.MockerFixture):
        fake_redis_url = faker.url(['redis'])
        fake_mongo_url = faker.url(['mongodb'])
        fake_mongodb_name = faker.first_name()

        mocker.patch.dict(os.environ, {
            'CMDPROXY_REDIS_URL': fake_redis_url,
            'CMDPROXY_MONGO_URL': fake_mongo_url,
            'CMDPROXY_MONGODB_NAME': fake_mongodb_name,
        })

        init_client_conf()

        conf = get_client_end_conf()

        assert conf.celery.broker_url == fake_redis_url
        assert conf.celery.backend_url == fake_mongo_url
        assert conf.cloud.mongodb_name == fake_mongodb_name

    def test_init_client_by_conf(self, faker, fake_local_file_maker):
        fake_redis_url = faker.url(['redis'])
        fake_mongo_url = faker.url(['mongodb'])
        fake_mongodb_name = faker.first_name()

        content = f'''
redis_url: {fake_redis_url}
mongo_url: {fake_mongo_url}
mongodb_name: {fake_mongodb_name}
'''

        conf_path = fake_local_file_maker(content.encode(), suffix='.yaml')

        init_client_conf(conf_path)

        conf = get_client_end_conf()

        assert conf.celery.broker_url == fake_redis_url
        assert conf.celery.backend_url == fake_mongo_url
        assert conf.cloud.mongodb_name == fake_mongodb_name

    def test_init_client_by_mixin(self, faker, mocker, fake_local_file_maker):
        fake_redis_url = faker.url(['redis'])
        fake_mongo_url = faker.url(['mongodb'])
        fake_mongodb_name = faker.first_name()

        content = f'''
redis_url: url-going-to-be-overwrite
mongo_url: {fake_mongo_url}
mongodb_name: name-going-to-be-overwrite
'''

        conf_path = fake_local_file_maker(content.encode(), suffix='.yaml')

        mocker.patch.dict(os.environ, {
            'CMDPROXY_MONGODB_NAME': fake_mongodb_name,
        })

        init_client_conf(conf_path, redis_url=fake_redis_url)

        conf = get_client_end_conf()

        assert conf.celery.broker_url == fake_redis_url
        assert conf.celery.backend_url == fake_mongo_url
        assert conf.cloud.mongodb_name == fake_mongodb_name


class TestServerConfig:
    def test_init_server_by_cli(self, faker, fake_local_file_maker):
        fake_redis_url = faker.url(['redis'])
        fake_mongo_url = faker.url(['mongodb'])
        fake_mongodb_name = faker.first_name()
        fake_command_palette = fake_local_file_maker()

        init_server_conf(redis_url=fake_redis_url, mongo_url=fake_mongo_url,
                         mongodb_name=fake_mongodb_name,
                         command_palette=fake_command_palette)

        conf = get_server_end_conf()

        assert conf.celery.broker_url == fake_redis_url
        assert conf.celery.backend_url == fake_mongo_url
        assert conf.cloud.mongodb_name == fake_mongodb_name
        assert conf.command_palette_path == fake_command_palette

    def test_init_server_by_env(self, faker, mocker: pytest_mock.MockerFixture,
                                fake_local_file_maker):
        fake_redis_url = faker.url(['redis'])
        fake_mongo_url = faker.url(['mongodb'])
        fake_mongodb_name = faker.first_name()
        fake_command_palette = fake_local_file_maker()

        mocker.patch.dict(os.environ, {
            'CMDPROXY_REDIS_URL': fake_redis_url,
            'CMDPROXY_MONGO_URL': fake_mongo_url,
            'CMDPROXY_MONGODB_NAME': fake_mongodb_name,
            'CMDPROXY_COMMAND_PALETTE': str(fake_command_palette),
        })

        init_server_conf()

        conf = get_server_end_conf()

        assert conf.celery.broker_url == fake_redis_url
        assert conf.celery.backend_url == fake_mongo_url
        assert conf.cloud.mongodb_name == fake_mongodb_name
        assert conf.command_palette_path == fake_command_palette

    def test_init_server_by_conf(self, faker, fake_local_file_maker):
        fake_redis_url = faker.url(['redis'])
        fake_mongo_url = faker.url(['mongodb'])
        fake_mongodb_name = faker.first_name()
        fake_command_palette = fake_local_file_maker()

        content = f'''
redis_url: {fake_redis_url}
mongo_url: {fake_mongo_url}
mongodb_name: {fake_mongodb_name}
command_palette: {str(fake_command_palette)}
'''

        conf_path = fake_local_file_maker(content.encode(), suffix='.yaml')

        init_server_conf(conf_path)

        conf = get_server_end_conf()

        assert conf.celery.broker_url == fake_redis_url
        assert conf.celery.backend_url == fake_mongo_url
        assert conf.cloud.mongodb_name == fake_mongodb_name
        assert conf.command_palette_path == fake_command_palette
