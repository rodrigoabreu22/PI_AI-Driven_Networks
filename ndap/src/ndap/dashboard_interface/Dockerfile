FROM python:3.12

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./src/ndap/dashboard_interface/ /app

EXPOSE 8074

CMD ["python", "data_consumer_api.py"]
