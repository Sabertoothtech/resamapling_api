FROM python:3
ENV PYTHONUNBUFFERED=1
WORKDIR /code
COPY requirements.txt /code/
COPY conn.py /code/
RUN pip install -r requirements.txt


COPY . /code/