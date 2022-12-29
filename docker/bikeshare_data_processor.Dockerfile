FROM python:3.10.9
RUN mkdir -p /app

COPY src/bikeshare_data_processor /app/src/bikeshare_data_processor

RUN pip install -r /app/src/bikeshare_data_processor/requirements.txt --no-cache-dir


ENV PYTHONPATH "/app/src"
ENTRYPOINT ["uvicorn"]
CMD ["bikeshare_data_processor.app:app", "--app-dir", "/app/src", "--workers", "1", "--host" ,"0.0.0.0", "--port", "8000"]