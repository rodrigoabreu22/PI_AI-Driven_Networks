import asyncio
import base64
import logging
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from kafka import KafkaProducer
from contextlib import asynccontextmanager
import uvicorn
import requests
from kafka.admin import KafkaAdminClient, NewTopic

SUBSCRIPTION_URL = "http://localhost:8071/nnwdaf-eventssubscription/v1/subscriptions"

TOPIC_PUSH = "RAW_NETWORK_DATA_RECEIVED"
BROKER = "localhost:29092"

packet_queue = asyncio.Queue()
producer = None

logging.basicConfig(
    filename='logs/data_receiver.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_topic(topic_name, broker, num_partitions=1, replication_factor=1):
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=broker)
        existing_topics = admin_client.list_topics()
        if topic_name in existing_topics:
            logging.info(f"Topic '{topic_name}' already exists.")
        else:
            topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            admin_client.create_topics([topic])
            logging.info(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        logging.error(f"Failed to create topic '{topic_name}': {e}")
    finally:
        try:
            admin_client.close()
        except Exception as e:
            logging.error(f"Can not close admin client. Error: {e}")
            pass

def create_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: v  # Raw bytes
    )
    logging.info("Kafka producer started.")
    return producer

async def kafka_worker():
    global producer

    while True:
        packet_data = await packet_queue.get()
        logging.info(f"Processing packet: {packet_data}")
        try:
            value = packet_data["value"]
            headers = packet_data["headers"]
            logging.info(f"headers: {headers}")
            producer.send(TOPIC_PUSH, value=value, headers=headers)

        except Exception as e:
            logging.error(f"Kafka send failed: {e}")

        packet_queue.task_done()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    create_topic(TOPIC_PUSH, BROKER)
    producer = create_kafka_producer()
    asyncio.create_task(kafka_worker())

    payload = {
        "event": "PDU_SESSION_TRAFFIC",
        "notificationURI": "http://localhost:8072/nwdaf-collector-notify"
    }
    response = requests.post(SUBSCRIPTION_URL, json=payload)

    if response.status_code != 201:
        logging.error(f"Failed to subscribe to NWDAF: {response.status_code} - {response.text}")
        raise RuntimeError(f"Failed to subscribe to NWDAF: {response.status_code} - {response.text}")
    
    logging.info(f"Response from NWDAF: {response.status_code} - {response.text}")
    
    yield

    producer.flush()
    producer.close()
    logging.info("Kafka producer shut down.")
    logging.info("Shutting down app.")


app = FastAPI(lifespan=lifespan)

@app.post("/nwdaf-collector-notify")
async def receive_data(data: dict):
    """
    Endpoint to receive data from the NWDAF collector.
    """
    try:
        packets = data.get("pcap", [])
        for pkt in packets:
            raw_bytes = base64.b64decode(pkt["pcap_bytes"])
            timestamp = pkt.get("timestamp", "")

            await packet_queue.put({
                "value": raw_bytes,
                "headers": [("timestamp", timestamp.encode("utf-8"))]
            })

        return JSONResponse(content={"status": "success"}, status_code=200)
    
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)
    
if __name__ == "__main__":
    uvicorn.run("data_receiver_api:app", host="0.0.0.0", port=8072, reload=False)