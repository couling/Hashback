FROM alpine:latest AS base
ARG version
RUN apk add --no-cache python3 py3-pip py3-wheel
RUN pip install --no-cache-dir --extra-index-url https://test.pypi.org/simple/ hashback==${version}
