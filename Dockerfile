FROM alpine:latest AS base
ARG version
ARG pip_extra=""
RUN apk add --no-cache python3 py3-pip py3-wheel
# pip_extra allows use of --pre and test pypi for build purposes

RUN pip install --no-cache-dir ${pip_extra} hashback[server]==${version}
COPY docs/examples/basic-server.json /etc/hashback/basic-server.json
RUN hashback-db-admin /data create


RUN apk add git


COPY / /app

RUN pip install -e /app[server]

VOLUME /data
VOLUME /etc/hashback
