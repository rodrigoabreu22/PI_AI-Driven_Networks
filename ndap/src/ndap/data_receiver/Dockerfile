FROM python:3.12

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./src/ndap/data_receiver /app

EXPOSE 8072

CMD ["python", "data_receiver_api.py"]