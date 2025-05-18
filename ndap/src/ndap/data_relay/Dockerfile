FROM python:3.12

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./src/ndap/data_relay/ /app

EXPOSE 8073

CMD ["python", "data_relay_api.py"]