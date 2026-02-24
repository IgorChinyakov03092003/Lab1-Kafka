FROM python:3.10-slim

WORKDIR /backend

COPY docker-requirements.txt .
RUN pip install --no-cache-dir -r docker-requirements.txt

COPY . .

CMD ["sleep", "infinity"]
