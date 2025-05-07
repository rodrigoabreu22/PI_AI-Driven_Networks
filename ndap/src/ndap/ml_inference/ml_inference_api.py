from fastapi import FastAPI, UploadFile, File
from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pickle
import threading
import logging
from contextlib import asynccontextmanager

from ml_inference import update_binary_model, start_kafka_inference_loop

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("Starting Kafka inference thread...")
    thread = threading.Thread(target=start_kafka_inference_loop, daemon=True)
    thread.start()
    yield  # Run FastAPI app
    logging.info("App shutdown. Kafka thread will stop when process exits.")

app = FastAPI(lifespan=lifespan)

@app.post("/update-model")
async def update_model(model_pickle: UploadFile = File(...)):
    try:
        contents = await model_pickle.read()
        model = pickle.loads(contents)
        update_binary_model(model)
        return {"message": "Model updated successfully"}
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

