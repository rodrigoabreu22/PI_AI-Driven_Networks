FROM python:3.12

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./src/ndap/data_producer/ /app

EXPOSE 8071

CMD ["python", "nwdaf_collector.py"]