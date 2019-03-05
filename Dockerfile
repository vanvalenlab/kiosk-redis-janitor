FROM ubuntu:18.04

RUN apt-get update && apt-get install -y \
    curl \
    python3 \
    python3-pip

RUN ln -s /usr/bin/python3 /usr/bin/python && ln -s /usr/bin/pip3 /usr/bin/pip

RUN pip install python-decouple \
    redis

RUN cd /usr/local/bin \
    && curl -O https://storage.googleapis.com/kubernetes-release/release/v1.6.2/bin/linux/amd64/kubectl \
    && chmod 755 /usr/local/bin/kubectl

COPY ./redis-janitor.py /

#CMD ["sleep", "10000"]
CMD ["python", "redis-janitor.py"]
