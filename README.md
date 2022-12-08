# CmdProxy

[![CmdProxy unit tests](https://github.com/limoiie/cmdproxy.py/actions/workflows/python-package.yml/badge.svg?branch=master)](https://github.com/limoiie/cmdproxy.py/actions?branch=master)

A framework for remote command proxy.

## Get started

### Install from the source

Open a terminal, and run:

```shell
python -m pip install git+https://github.com/limoiie/cmdproxy.py.git
```

## Examples

You can try a simple example using docker.

First, launch the server in daemon mode:

```shell
docker compose --profile server up -d
```

Then, run the client example:

```shell
docker compose run --rm client
```