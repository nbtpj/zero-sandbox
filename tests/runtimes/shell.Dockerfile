# A fixture, not a catalog: the suite submits Dockerfiles and the server builds whatever arrives.
FROM docker.io/library/alpine:latest
ENV ZERO_RUNTIME_SHELL=/bin/sh
