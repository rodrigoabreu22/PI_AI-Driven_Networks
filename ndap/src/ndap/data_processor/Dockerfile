FROM python:3.12

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Install prerequisites
RUN apt-get update && apt-get install -y wget gnupg lsb-release

# Download and install the apt-ntop package
RUN wget http://apt.ntop.org/20.04/all/apt-ntop.deb && \
    dpkg -i apt-ntop.deb && \
    apt-get update && \
    apt-get install -f -y

RUN apt-get install -y nprobe

COPY ./src/ndap/data_processor/data_processor.py /app
COPY ./src/ndap/data_processor/data_processor_consumer.py /app
COPY ./src/ndap/data_processor/features/ /app/features
COPY ./src/ndap/data_processor/extractedInitial.flows /app
COPY ./src/ndap/data_processor/NUSW-NB15_GT.csv /app

CMD ["python", "data_processor_consumer.py"]