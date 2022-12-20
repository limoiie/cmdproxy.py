import tempfile

from cmdproxy import Client, Param, init_client_conf


def main(redis_url=None, mongo_url=None, mongodb_name=None):
    init_client_conf(redis_url=redis_url, mongo_url=mongo_url,
                     mongodb_name=mongodb_name)
    client = Client.instance()

    with tempfile.NamedTemporaryFile() as in_file, \
            tempfile.NamedTemporaryFile() as out_file, \
            tempfile.NamedTemporaryFile() as stdout, \
            tempfile.NamedTemporaryFile() as stderr:
        content = b'some random string...'
        in_file.write(content)
        in_file.flush()

        ret_code = client.run(
            Param.remote_env('sh'), [
                '-c',
                Param.format('cat {input} > {output}', {
                    'input': Param.ipath(in_file.name),
                    'output': Param.opath(out_file.name)
                }),
            ],
            stdout=Param.opath(stdout.name),
            stderr=Param.opath(stderr.name),
            env=None,
            cwd=None
        )

        print('Checking return code...')
        assert ret_code == 0

        print('Checking output...')
        assert content == out_file.read()

        print('Checking stdout...')
        assert b'' == stdout.read()

        print('Checking stderr...')
        assert b'' == stderr.read()

        print('All passed, exit')


if __name__ == '__main__':
    main()
