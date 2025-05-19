import asyncio
import logging
import os
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from influxdb_client import InfluxDBClient, Point, WriteOptions, WritePrecision
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
import uvicorn

load_dotenv()

NWDAF_URL = "http://data_relay:8073/nnwdaf-eventssubscription/v1/subscriptions"

# InfluxDB
INFLUXDB_URL = "http://influxdb_processed:8086"
INFLUXDB2_TOKEN = os.getenv("INFLUXDB2_TOKEN")
INFLUXDB2_ORG = os.getenv("INFLUXDB2_ORG")
INFLUXDB_BUCKET = "processed_data"

data_queue = asyncio.Queue()
subscription_ready_event = asyncio.Event()

# Logging
logging.basicConfig(
    filename='logs/data_consumer_api.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# InfluxDB client
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB2_TOKEN, org=INFLUXDB2_ORG)
write_api = influx_client.write_api(write_options=WriteOptions(batch_size=1))

async def influx_worker():
    await subscription_ready_event.wait()  # Block until subscription is ready
    logging.info("InfluxDB worker started and waiting for data.")

    while True:
        try:
            record = await data_queue.get()

            timestamp = datetime.now(timezone.utc).isoformat()
            point = Point("network_processed_data").time(timestamp, WritePrecision.NS).tag("source", "NWDAF")

            for key, value in record.items():
                if isinstance(value, (int, float)):
                    point.field(key, value)
                else:
                    point.tag(key, str(value))

            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB2_ORG, record=point)

        except Exception as e:
            logging.error(f"Failed to write to InfluxDB: {e}")

        finally:
            data_queue.task_done()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("Starting lifespan context manager...")
    asyncio.create_task(influx_worker())

    # Subscribe to NWDAF
    payload = {
        "event": "PDU_SESSION_TRAFFIC",
        "notificationURI": "http://data_consumer:8074/data-consumer-notify"
    }

    logging.info("Subscribing to NWDAF...")

    response = requests.post(NWDAF_URL, json=payload)

    if response.status_code != 201:
        logging.error(f"Subscription failed: {response.status_code} - {response.text}")
        raise RuntimeError(f"Failed to subscribe to NWDAF: {response.status_code} - {response.text}")

    logging.info(f"Successfully subscribed to NWDAF: {response.text}")

    subscription_ready_event.set()

    yield

    influx_client.close()
    logging.info("Shut down InfluxDB client.")

app = FastAPI(lifespan=lifespan)

@app.post("/data-consumer-notify")
async def data_consumer_notify(data: dict):
    try:
        features = data.get("features", [])

        for record in features:
            await data_queue.put(record)

        return JSONResponse(content={"status": "success"}, status_code=200)
    
    except Exception as e:
        logging.error(f"Error processing received data: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)

@app.get("/health")
def health():
    return {"status": "OK"}

if __name__ == "__main__":
    uvicorn.run("data_consumer_api:app", host="0.0.0.0", port=8074, reload=False)
