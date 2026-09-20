FROM python:3.12-slim
RUN pip install --no-cache-dir fastapi==0.115.6 "uvicorn[standard]==0.34.0" redis==5.2.1 asyncpg==0.30.0 httpx==0.28.1
WORKDIR /srv
COPY app.py pricing_svc.py loadgen.py minihttp.py ./
