import sys
import time
import pandas as pd
import pandasql as psql
import json

def test_enviroment(enviroment, start_response):
  headers = [('Content-Type', 'text/html')]
  start_response('200 OK', headers)
  response = {}

  selected_movies_columns = ['belongs_to_collection', 'popularity', 'release_year', 'return']
  selected_credits_columns = ['id']

  start = time.perf_counter()
  df_credits = pd.read_parquet("datasets/credits_dataset.parquet", columns=selected_credits_columns)
  end = time.perf_counter()
  #print(f"Tiempo de lectura del parquet credits: {end - start:0.4f} segundos")
  response.update({"credits": f"Tiempo de lectura del parquet credits: {end - start:0.4f} segundos"})


  start = time.perf_counter()
  df_movies = pd.read_parquet("datasets/movies_dataset.parquet", columns=selected_movies_columns)
  end = time.perf_counter()
  #print(f"Tiempo de lectura del parquet movies: {end - start:0.4f} segundos\n")
  response.update({"movies": f"Tiempo de lectura del parquet movies: {end - start:0.4f} segundos"})

  #print(f"tamaño dataframe credits: {sys.getsizeof(df_credits)/1024/1024} MB")
  response.update({"credits_size": f"tamaño dataframe credits: {sys.getsizeof(df_credits)/1024/1024} MB"})
  #print(f"tamaño dataframe movies: {sys.getsizeof(df_movies)/1024/1024} MB\n")
  response.update({"movies_size": f"tamaño dataframe movies: {sys.getsizeof(df_movies)/1024/1024} MB"})

  #print(df_credits.columns)
  #print("\n")
  #print(df_movies.columns)

  #print("\n\n*** using pandas query")
  #print(df_movies.query("release_year == 2015"))

  #print("\n\n*** using pandasql")
  sql_result =psql.sqldf("SELECT * FROM df_movies WHERE release_year = 2015")
  response.update({"sql_result": sql_result.to_json(orient='records')})
  return [bytes(json.dumps(response), 'utf-8')]
