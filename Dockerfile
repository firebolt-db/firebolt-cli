FROM python:3.10

RUN pip3 install firebolt-cli

ENTRYPOINT ["firebolt"]
