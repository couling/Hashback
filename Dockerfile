FROM alpine:latest AS base
RUN --mount=type=cache,sharing=locked,target=/var/cache/apk \
    apk add python3 py3-pip py3-wheel

FROM base as build
COPY /dist/ /hashback/

FROM base as final

RUN --mount=type=cache,sharing=locked,target=/root/.cache \
    --mount=type=bind,from=build,source=/hashback/,target=/mnt \
    file=$(ls -1 /mnt) ; pip install /mnt/${file}[server]

COPY docs/examples/basic-server.json /etc/hashback/basic-server.json
RUN hashback-db-admin /data create

VOLUME /data
VOLUME /etc/hashback
