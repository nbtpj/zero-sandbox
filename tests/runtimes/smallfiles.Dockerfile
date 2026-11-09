# The property Mathlib actually stresses, without the 5 GB download: an image whose tree is tens
# of thousands of small files, so `docker run` assembles a large overlay on every launch.
FROM docker.io/library/alpine:latest
RUN mkdir -p /opt/tree && cd /opt/tree \
    && awk 'BEGIN{for(i=0;i<30000;i++){fn="f" i; print "x" > fn; close(fn)}}' \
    && ls /opt/tree | wc -l
ENV ZERO_RUNTIME_SHELL=/bin/sh
