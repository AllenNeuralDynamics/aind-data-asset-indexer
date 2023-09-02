# hash:sha256:9a022f06a6be39263eaed43ddc3a403055d5e0787ca58a7bfb358336c083fe0e
FROM registry.codeocean.allenneuraldynamics-test.org/codeocean/jupyterlab:3.0.9-miniconda4.9.2-python3.8-ubuntu20.04

ARG DEBIAN_FRONTEND=noninteractive

ARG GIT_ASKPASS
ARG GIT_ACCESS_TOKEN
COPY git-askpass /

COPY postInstall /
RUN /postInstall
