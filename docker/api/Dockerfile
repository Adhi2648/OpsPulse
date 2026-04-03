FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir \
    fastapi \
    uvicorn[standard] \
    sqlalchemy \
    psycopg[binary] \
    pydantic-settings \
    python-dotenv && \
    pip install --no-cache-dir -e . --no-deps

EXPOSE 8000

CMD ["uvicorn", "opspulse.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
