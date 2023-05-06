# hash:sha256:23c99d137f9bb59ed88a098a148183941db77f41acdf7d6081d664b84b495abd
FROM registry.codeocean.allenneuraldynamics.org/codeocean/jupyterlab:3.4.4-miniconda4.12.0-python3.9-ubuntu20.04

ARG DEBIAN_FRONTEND=noninteractive

ARG GIT_ASKPASS
ARG GIT_ACCESS_TOKEN
COPY git-askpass /

RUN pip install -U --no-cache-dir \
    aind-codeocean-api==0.2.1 \
    aind-data-access-api[full]==0.2.1 \
    python-dotenv==1.0.0
