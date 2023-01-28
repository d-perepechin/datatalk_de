FROM python:3.9

RUN pip install pandas sqlalchemy psycopg2-binary

WORKDIR /app

COPY insert_data.py insert_data.py

ENTRYPOINT [ "python", "insert_data.py" ]