# hash:sha256:84624b27efe1d2e99a9deb9ae0cd41e4c523967f6be7fc79c7a17cb2d3b51785
FROM registry.codeocean.allenneuraldynamics.org/codeocean/jupyterlab:3.4.4-miniconda4.12.0-python3.9-ubuntu20.04

ARG DEBIAN_FRONTEND=noninteractive

ARG GIT_ASKPASS
ARG GIT_ACCESS_TOKEN
COPY git-askpass /

RUN pip install -U --no-cache-dir \
    aind-codeocean-api==0.2.1 \
    aind-data-access-api==0.2.0 \
    python-dotenv==1.0.0
