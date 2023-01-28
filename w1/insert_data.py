#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from sqlalchemy import create_engine
import argparse

def main(params):
	user = params.user
	password = params.password
	host = params.host
	port = params.port
	db = params.db
	table_name = params.table_name
	csv = params.csv


	engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
	engine.connect()
	print('engine created')

	df_iter = pd.read_csv(csv, nrows=1)
	print('csv readed')

	df = next(df_iter)
	for col in df.columns:
		if 'date' in col:
			df[col] = pd.to_datetime(df[col])
			df[col] = pd.to_datetime(df[col])

	df.head(0).to_sql(table_name, index=False, con=engine, if_exists='replace')
	print('table created')

	df_iter = pd.read_csv(csv, chunksize=100_000)
	while True:
		df = next(df_iter)
		for col in df.columns:
			if 'date' in col:
				df[col] = pd.to_datetime(df[col])
				df[col] = pd.to_datetime(df[col])
		df.to_sql(table_name, index=False, con=engine, if_exists='append')
		print('inserting...')



if __name__ == '__main__':

	parser = argparse.ArgumentParser(
			prog = 'dockerIngestion',
			description = ' ingestion csv to postgres',)

	parser.add_argument('--user', help='user name for pgsql')           # positional argument
	parser.add_argument('--password', help='password name for pgsql')
	parser.add_argument('--host', help='host name for pgsql')
	parser.add_argument('--port', help='port name for pgsql')
	parser.add_argument('--db', help='db name for pgsql')
	parser.add_argument('--table_name', help='tablename where to write')
	parser.add_argument('--csv', help='url of csv file')

	args = parser.parse_args()
	main(args)


# python insert_data.py --user=passwort=root --host=localhost --port=5432 --db=ny_taxi --table_name=yellow_tripdata_trip --csv"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
# docker run -it taxi_ingest:v001 --user=passwort=root --host=localhost --port=5432 --db=ny_taxi --table_name=yellow_tripdata_trip --csv"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"