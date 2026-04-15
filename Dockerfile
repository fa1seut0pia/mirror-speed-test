FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    MST_HOST=0.0.0.0 \
    MST_PORT=8080

WORKDIR /app

RUN python -m pip install --no-cache-dir --upgrade pip certifi

ARG APP_VERSION=dev
RUN printf '%s\n' "${APP_VERSION}" > VERSION

COPY app.py index.html ./

RUN useradd --create-home --shell /usr/sbin/nologin appuser \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8080

CMD ["python", "app.py"]
