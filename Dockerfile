FROM python:3.10-slim

WORKDIR /app

ADD src ./src
ADD .env.template .
ADD pyproject.toml .
ADD setup.py .

RUN apt-get update

RUN pip install . --no-cache-dir

CMD ["python", "main.py"]
