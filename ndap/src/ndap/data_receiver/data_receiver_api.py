import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import logging
from contextlib import asynccontextmanager
import uvicorn

SUBSCRIPTION_URL = "http://localhost:8071/nnwdaf-eventssubscription/v1/subscriptions"

logging.basicConfig(
    filename='logs/data_receiver.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@asynccontextmanager
async def lifespan(app: FastAPI):
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

    logging.info("Shutting down app.")


app = FastAPI(lifespan=lifespan)

@app.post("/nwdaf-collector-notify")
def receive_data(data: dict):
    """
    Endpoint to receive data from the NWDAF collector.
    """
    try:
        # Process the incoming data
        logging.info(f"Received data: {data}")
        # Here you can add your logic to handle the received data
        return JSONResponse(content={"status": "success"}, status_code=200)
    
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)
    
if __name__ == "__main__":
    uvicorn.run("data_receiver_api:app", host="0.0.0.0", port=8072, reload=False)