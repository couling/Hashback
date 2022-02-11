FROM alpine:latest AS base
RUN --mount=type=cache,sharing=locked,target=/etc/apk/cache \
    apk add --no-cache python3 py3-pip py3-wheel


FROM base as build
COPY / /hashback/
RUN --mount=type=cache,sharing=locked,target=/etc/apk/cache \
    --mount=type=cache,sharing=locked,target=/root/.cache \
    if ! [ -f /hashback/dist/*.whl ] ; then \
      apk add git && \
      python3 -m pip install --upgrade pip wheel build setuptools && \
      python3 -m build /hashback --wheel ; \
    fi


FROM base as final

RUN --mount=type=cache,sharing=locked,target=/root/.cache \
    --mount=type=bind,from=build,source=/hashback/,target=/mnt \
    file=$(ls -1 /mnt/dist/*.whl) ; pip install ${file}[server]

COPY docs/examples/basic-server.json /etc/hashback/basic-server.json
RUN hashback-db-admin /data create

VOLUME /data
VOLUME /etc/hashback
