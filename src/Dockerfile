FROM python:3.11-alpine
ENV PYTHONUNBUFFERED 1

ARG APP_VERSION=dev
ARG GIT_COMMIT=unknown
ARG GIT_BRANCH=unknown
ARG BUILD_DATE=unknown

ENV APP_VERSION=${APP_VERSION}
ENV GIT_COMMIT=${GIT_COMMIT}
ENV GIT_BRANCH=${GIT_BRANCH}
ENV BUILD_DATE=${BUILD_DATE}

LABEL org.opencontainers.image.version=${APP_VERSION}
LABEL org.opencontainers.image.revision=${GIT_COMMIT}
LABEL org.opencontainers.image.created=${BUILD_DATE}
LABEL org.opencontainers.image.source_branch=${GIT_BRANCH}

WORKDIR /solarflow/
COPY ./requirements.txt /solarflow/requirements.txt

RUN python -m venv --system-site-packages /opt/venv

ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONPATH="/usr/lib/python3.11/site-packages"

RUN pip install -Ur requirements.txt && \
    rm -rf /root/.cache/ && \
    rm -rf /var/cache/apk/*

COPY solarflow /solarflow/
ENTRYPOINT ["python3", "solarflow-control.py"]