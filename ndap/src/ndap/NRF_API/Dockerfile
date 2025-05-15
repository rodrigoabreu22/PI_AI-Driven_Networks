FROM python:3.12

WORKDIR /app

RUN pip install --no-cache-dir fastapi uvicorn

COPY ./src/ndap/NRF_API/ /app

EXPOSE 8070

CMD ["python", "nrf.py"]