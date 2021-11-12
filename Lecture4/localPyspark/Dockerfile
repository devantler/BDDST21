FROM bde2020/spark-submit:3.1.1-hadoop3.2

COPY template.sh /

COPY . /app/
WORKDIR /app
RUN pip install -r requirements.txt

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/entrypoint.py

CMD ["/bin/bash", "/template.sh"]