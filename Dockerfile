FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py admin.py relais_ins.py ./
COPY .streamlit/config.toml .streamlit/config.toml
RUN useradd --create-home appuser && chown -R appuser:appuser /app
USER appuser
EXPOSE 8501
CMD ["streamlit","run","app.py","--server.address=0.0.0.0","--server.port=8501"]
