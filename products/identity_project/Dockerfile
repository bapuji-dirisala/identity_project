# Multi-stage build image used by the Jenkins unit-test step.
# Produces a slim python 3.11 image with JDK 11 (needed by PySpark),
# Docker CLI (for nested docker ops), node + make + poetry.

FROM hashicorp/terraform:1.9.8 as terraform
WORKDIR /spark/python
COPY . .
RUN chmod -R 755 .

FROM openjdk:11-jdk-slim as java

FROM python:3.11-slim as python
COPY --from=java /usr/local/openjdk-11 /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk

USER root
RUN apt-get update && apt-get install -y curl software-properties-common build-essential jq make \
 && curl -sSL https://get.docker.com/ | sh \
 && curl -sL https://deb.nodesource.com/setup_19.x | bash - \
 && apt-get install -y nodejs \
 && pip install poetry

# Non-root user (fixes IaC "Last User Is root" security finding).
RUN groupadd --system --gid 1001 appuser \
 && useradd  --system --uid 1001 --gid appuser --shell /bin/bash --create-home appuser
USER appuser
