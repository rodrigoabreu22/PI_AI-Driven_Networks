FROM python:3.12

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./src/ndap/ml_inference/ /app

EXPOSE 9050

CMD ["python", "ml_inference_api.py"]