FROM python:3.10-slim

ENV ENV_DISABLE_DONATION_MSG=1

WORKDIR /app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir reports

COPY inventory_and_sales.py ./
COPY load.py ./
COPY token.json ./
COPY credentials.json ./
COPY credentials.py ./
COPY product_groups.py ./

CMD ["sh", "-c", "python inventory_and_sales.py && python load.py"]
