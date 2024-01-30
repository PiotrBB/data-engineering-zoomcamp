FROM python:3.11

RUN apt-get install wget 
RUN pip install pandas sqlalchemy psycopg2-binary pyarrow

WORKDIR /app
COPY taxi_pipeline.py taxi_pipeline.py

ENTRYPOINT [ "python", "taxi_pipeline.py" ]
