FROM python:3-slim
WORKDIR /app

# Pip install
ADD code ./src
ADD pyproject.toml .
ADD setup.py .
RUN pip install . --no-cache-dir

CMD ["python", "main.py"]
