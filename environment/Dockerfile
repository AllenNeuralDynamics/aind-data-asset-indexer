# hash:sha256:1eeb3e34cd8b5e4e8a1bf46f524bdf4de7ab2399ebad5ab18a5037f5e640df6f
FROM registry.codeocean.allenneuraldynamics.org/codeocean/jupyterlab:3.4.4-miniconda4.12.0-python3.9-ubuntu20.04

ARG DEBIAN_FRONTEND=noninteractive

ARG GIT_ASKPASS
ARG GIT_ACCESS_TOKEN
COPY git-askpass /

RUN pip install -U --no-cache-dir \
    aind-codeocean-api \
    aind-data-access-api \
    python-dotenv
