FROM python:3.12-slim

WORKDIR /app

ARG JM_IMAGE_TAG=dev

RUN apt-get update \
    && apt-get install -y --no-install-recommends restic ca-certificates curl unzip \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://rclone.org/install.sh | bash

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY jm_manager /app/jm_manager
COPY templates /app/templates
COPY static /app/static
COPY deploy/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

ENV PYTHONUNBUFFERED=1
ENV JM_IMAGE_TAG=${JM_IMAGE_TAG}

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "-m", "jm_manager"]
