# Declares the one capability a request may then select: a single HTTP(S) endpoint.
FROM docker.io/library/alpine:latest
RUN apk add --no-cache curl iproute2
ENV ZERO_RUNTIME_SHELL=/bin/sh ZERO_RUNTIME_EGRESS=endpoint
