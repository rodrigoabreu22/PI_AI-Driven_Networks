# PI_AI-Driven_Networks
Projeto de Informática - 2024/2025

# Documentation

Github repository: https://github.com/rodrigoabreu22/PI_Microsite

Microsite: https://rodrigoabreu22.github.io/PI_Microsite/

Project Backlog: https://github.com/users/rodrigoabreu22/projects/4/views/1

## Abstract

Modern networks have evolved from static infrastructures into dynamic, intelligent, and adaptive systems. 5G and Beyond 5G networks need to handle large volumes of data, support a wide variety of applications, and ensure high reliability and low latency. However, increased data flow can degrade network performance, and usage spikes may compromise service quality.

To address these challenges, our work introduces a scalable and modular MLOps pipeline that integrates ML and automation directly into the 5G Core architecture. This approach enhances network optimization, improves decision-making, and ensures resilient performance even in high-demand conditions.

## Architecture

![Alt text](https://rodrigoabreu22.github.io/PI_Microsite/assets/images/arquitetura-8c34d7c0e58dfba6ef7ebb16d532fc4f.png)

## Promotional Video

<iframe width="560" height="315" src="https://www.youtube.com/embed/lXgchBfzUZE" frameborder="0" allowfullscreen></iframe>

## Poster

![Alt text](https://rodrigoabreu22.github.io/PI_Microsite/img/PosterPI.png)

## Project Team

| Name | Email | NMEC | Role |
| ---- | ----- | ---- | ---- |
| Rodrigo Abreu | rodrigo.abreu@ua.pt | 113626 | Team Manager Back end Engineer |
| Hugo Ribeiro | hugo.ribeiro04@ua.pt | 113402​ | Data Engineer UI Engineer Messaging & API Engineer |
| João Neto | jneto04@ua.pt | 113482 | Data Engineer Messaging & API Engineer |
| Eduardo Lopes | eduardolplopes@ua.pt | 103070 | DevOps Master, Architect |
| Jorge Domingues | jorgeguilherme@ua.pt | 113278 | Integration Engineer UI Engineer |

## Running 

### Requirements

Ensure you have [Docker]([https://](https://www.docker.com/)) installed, with [Docker Compose]([https://](https://docs.docker.com/compose/)) available.

Go to the ndap directory:

You will need to have the following .env file in the directory above.

Also install the necessary requirements:

```bash
pip install -r requirements.txt
```

It is also necessary to have the .pcap files. Start by criating a dataset_files directory. Click [here]([https://](https://unsw-my.sharepoint.com/personal/z5025758_ad_unsw_edu_au/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Fz5025758%5Fad%5Funsw%5Fedu%5Fau%2FDocuments%2FUNSW%2DNB15%20dataset%2Fpcap%20files%2Fpcaps%2017%2D2%2D2015&ga=1)) to download the .pcap files used by our team.

### .env

```txt
# InfluxDB
INFLUXDB_URL=http://localhost:8086
INFLUXDB_PORT=8086:8086
INFLUXDB_USERNAME=admin
INFLUXDB_PASSWORD=cbe25cf9cfe73cf040ae2f32c5d578d848a469bcc679c9c9e4281e32f57186af
INFLUXDB_BUCKET=raw_data
INFLUXDB2_BUCKET=processed_data
INFLUXDB_TOKEN=""
INFLUXDB_ORG=pi_14
INFLUXDB2_ORG=pi_14
INFLUXDB2_TOKEN=""

# Clickhouse
CLICKHOUSE_PORT_HTTP=8123:8123
CLICKHOUSE_PORT_NATIVE=9000:9000
CLICKHOUSE_DB=default
CLICKHOUSE_USER=network
CLICKHOUSE_PASSWORD= network25pi
```

### Setup

Start docker:

```bash
docker compose up -d --build
```

Go to [Raw DB]([http://](http://localhost:8087)) or to localhost:8087. Insert the following information:

```txt
USERNAME=admin
PASSWORD=cbe25cf9cfe73cf040ae2f32c5d578d848a469bcc679c9c9e4281e32f57186af
BUCKET=raw_data
ORG=pi_14
```

Retrive the token and add It to the .env file

```txt
INFLUXDB_TOKEN=""
```

Go to [Dashboard DB]([http://](http://localhost:8086)) or to localhost:8086. Insert the following information:

```txt
USERNAME=admin
PASSWORD=cbe25cf9cfe73cf040ae2f32c5d578d848a469bcc679c9c9e4281e32f57186af
BUCKET=processed_data
ORG=pi_14
```

Retrive the token and add It to the .env file

```txt
INFLUXDB2_TOKEN=""
```

Stop docker:

```bash
docker compose down
```

Restart docker:

```bash
docker compose up -d --build
```

To access the dashboard interfece (Chronograf) click [here]([http://](http://localhost:8888)) or go to localhost:8888

You will need to import the following [Json file](<ndap/dashboards.json>) to have access to our dashboards