FROM apache/spark-py:v3.3.1

USER root:root

RUN mkdir /app

COPY ./app/ /app/
COPY requirements.txt /app/requirements.txt
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

RUN pip install -r /app/requirements.txt

WORKDIR /app/
ENV PYSPARK_PYTHON=/usr/bin/python3

CMD ["python3","main.py"]