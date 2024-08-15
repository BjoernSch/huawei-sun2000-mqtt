FROM python:3.11-slim-buster

WORKDIR /huawei2mqtt

COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY .env.template /huawei2mqtt/.env
COPY /src/huawei2mqtt.py /huawei2mqtt

CMD ["python", "-u", "huawei2mqtt.py"]
