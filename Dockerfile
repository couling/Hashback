FROM alpine:latest AS base
ARG version
ARG pip_extra=""
RUN apk add --no-cache python3 py3-pip py3-wheel
#
RUN pip install --no-cache-dir hashback==${version} ${pip_extra}
