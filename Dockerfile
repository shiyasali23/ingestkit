FROM python:3.12-slim AS build_stage

WORKDIR /build
COPY pyproject.toml README.md ./
COPY ingestkit ./ingestkit
RUN pip install --no-cache-dir --prefix=/install ".[postgres,kubernetes]"

FROM python:3.12-slim

RUN useradd --create-home --uid 10001 ingestkit_user
COPY --from=build_stage /install /usr/local
COPY kubernetes/worker_job.yaml /app/kubernetes/worker_job.yaml

WORKDIR /app
USER ingestkit_user
ENTRYPOINT ["ingestkit"]
CMD ["--help"]
