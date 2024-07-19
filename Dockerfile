FROM python:3.10-slim

ENV ENV_DISABLE_DONATION_MSG=1

WORKDIR /app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY et.py ./
COPY main.py ./
COPY l.py ./
COPY keys keys/
COPY product_info product_info/

CMD ["python", "main.py"]
