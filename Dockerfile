FROM alpine:latest AS base
ARG version
ARG pip_extra=""
RUN apk add --no-cache python3 py3-pip py3-wheel
# pip_extra allows use of --pre and test pypi for build purposes
RUN pip install --no-cache-dir ${pip_extra} hashback==${version}
