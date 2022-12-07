from typing import cast

import celery

from cmdproxy import client, ipath, opath


def main(redis_url, mongo_url):
    conf = client.startup_app(redis_url=redis_url, mongo_url=mongo_url)

    from cmdproxy.celery_app.tasks import run
    res = client.Client(conf, cast(celery.Task, run)).run(
        'echo', [
            '-x=10',
            ('-i=%s', ipath(r'./local-path.i')),
            ('-o=%s', opath(r'./local-path.o'))
        ],
        stdout=ipath(r'./local-path.stdout'),
        stderr=opath(r'./local-path.stderr'),
        env={},
        cwd='./server-relative-path/'
    )

    print(f'ret code: {res.return_code}')


if __name__ == '__main__':
    pass
