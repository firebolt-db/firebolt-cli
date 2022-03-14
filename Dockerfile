FROM python:3.10

RUN pip3 install --no-cache-dir --upgrade firebolt-cli

ENTRYPOINT ["firebolt"]
