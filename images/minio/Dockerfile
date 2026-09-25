FROM bitnamilegacy/minio:2025.7.23-debian-12-r3

USER root

ENV MINIO_ACCESS_KEY {{ conf.s3.access_key_id }}
ENV MINIO_SECRET_KEY {{ conf.s3.access_secret_key }}

# ClickHouse checks that these buckets exist during startup.
RUN mkdir -p \
    /export/{{ conf.s3.bucket }} \
    /export/{{ conf.s3.cloud_storage_bucket }}-01 \
    /export/{{ conf.s3.cloud_storage_bucket }}-02

ENTRYPOINT ["minio"]

HEALTHCHECK --interval=500ms --timeout=1s --retries=60 \
    CMD curl --fail --silent http://localhost:9000/minio/health/ready

EXPOSE 9000 9001

CMD ["server", "/export", "--console-address", ":9001"]
