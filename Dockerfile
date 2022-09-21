FROM python:3.8 AS base

WORKDIR /code
COPY . .
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM postgres:12.1 AS database