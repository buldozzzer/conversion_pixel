FROM python:3.10

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.6.1 \
    POETRY_HOME="/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PYSETUP_PATH="/pysetup" \
    VENV_PATH="/pysetup/.venv"

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

RUN apt update && apt install -y iputils-ping

COPY config.ini .

COPY __init__.py .

COPY gunicorn_config.py .

COPY app.py .

COPY clickhouse_client.py .

EXPOSE 8080

CMD ["gunicorn", "--config", "gunicorn_config.py", "app:app"]
