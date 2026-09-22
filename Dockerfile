FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY b ./b
COPY database.py ./database.py

EXPOSE 9999/udp

CMD ["python", "b", "--host", "0.0.0.0", "--port", "9999"]
