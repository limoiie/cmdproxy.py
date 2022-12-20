FROM python:latest

WORKDIR /cmdproxy
COPY . /cmdproxy

RUN python -m pip install .

ENTRYPOINT ["python", "-m", "cmdproxy", "--command-palette=./examples/command-palette.yaml"]
