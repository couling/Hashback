FROM alpine:latest AS base
ARG version
RUN apk add --no-cache python3 py3-pip py3-wheel &&\
    pip install --no-cache-dir --extra-index-url https://test.pypi.org/simple/ hashback==${version} &&\
    pip apk del py3-pip py3-wheel
