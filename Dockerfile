# Container for an always-on host. The host must not suspend or scale to zero.
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=8501 \
    APP_FILE=app.py \
    HOME=/home/appuser
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir -r requirements.txt \
    && useradd --uid 10001 --create-home --shell /usr/sbin/nologin appuser \
    && mkdir -p /app/.streamlit /app/.local /home/appuser/.cache \
    && chown -R appuser:appuser /app /home/appuser
COPY --chown=appuser:appuser app.py admin.py relais_ins.py /app/
COPY --chown=appuser:appuser .streamlit/config.toml /app/.streamlit/config.toml
COPY --chown=appuser:appuser deploy/entrypoint.py deploy/healthcheck.py /app/deploy/
# Optional respondent help; no secrets or responses are copied into the image.
COPY --chown=appuser:appuser docs/ /app/docs/
USER 10001:10001
EXPOSE 8501
HEALTHCHECK --interval=30s --timeout=6s --start-period=45s --retries=3 \
  CMD ["python", "/app/deploy/healthcheck.py"]
ENTRYPOINT ["python", "/app/deploy/entrypoint.py"]
