FROM python:3.12

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./src/ndap/data_processor/data_processor.py /app
COPY ./src/ndap/data_processor/data_processor_consumer.py /app
COPY ./src/ndap/data_processor/features/ /app
COPY ./src/ndap/data_processor/extractedInitial.flows /app
COPY ./src/ndap/data_processor/NUSW-NB15_GT.csv /app

CMD ["python", "data_processor_consumer.py"]