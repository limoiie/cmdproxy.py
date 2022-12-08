FROM python:latest

COPY . /cmdproxy
RUN python -m pip install /cmdproxy

ENTRYPOINT ["python", "-m", "cmdproxy"]
