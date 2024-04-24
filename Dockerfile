FROM python:3.10-slim

WORKDIR /app

ADD src ./src
ADD pyproject.toml .
ADD setup.py .

RUN apt-get update
RUN pip install . --no-cache-dir
RUN pip install awscli

CMD ["./src/aind_data_asset_indexer/run.sh"]
