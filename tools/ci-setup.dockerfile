FROM ubuntu:16.04
ARG TARGET
RUN apt-get update -y
RUN apt-get install -y curl
RUN cd /root && curl https://bootstrap.pypa.io/get-pip.py
RUN cd /root && python get-pip.py
RUN pip install -U bindep tox
ADD . /root/src
RUN cd /root/src && bindep
RUN cd /root/src && tox -e$TARGET
