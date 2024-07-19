FROM bitnami/spark:3.2.1

USER root

RUN apt-get update && apt-get install -y python3 python3-pip

RUN apt-get install -y libpq-dev

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt
