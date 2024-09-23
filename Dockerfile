FROM python:3.10-slim

WORKDIR /app

ADD src ./src
ADD scripts ./scripts
ADD pyproject.toml .
ADD setup.py .

RUN apt-get update
RUN pip install . --no-cache-dir
RUN pip install awscli

RUN chmod +x ./scripts/run.sh
CMD ["./scripts/run.sh"]
