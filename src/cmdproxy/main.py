import fire

from cmdproxy import init_server_conf


def launch(redis_url: str, mongo_url: str, mongodb_name: str,
           command_palette_path: str):
    init_server_conf(redis_url, mongo_url, mongodb_name, command_palette_path)

    from cmdproxy.celery_app.app import app
    app.start()


if __name__ == '__main__':
    fire.Fire(launch)
