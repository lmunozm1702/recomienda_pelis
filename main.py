import sys
import time
import pandas as pd
import pandasql as psql
import json
import ast

from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def root():
  return {"message": "Hello World"}

# def cantidad_filmaciones_mes( Mes ): Se ingresa un mes en idioma Español. 
# Debe devolver la cantidad de películas que fueron estrenadas en el mes 
# consultado en la totalidad del dataset.
@app.get("/cantidad_filmaciones_mes")
async def cantidad_filmaciones_mes(mes: int):
  if mes < 1 or mes > 12:
    return {"mes": mes, "error": "Mes no existe"}
  
  #transformar el mes a string de 2 digitos
  if mes < 10:
    mes = "0" + str(mes)
  else:
    mes = str(mes)
  
  selected_movies_columns = ['release_date'] 
  start = time.perf_counter()  
  df_movies = pd.read_parquet("datasets/movies_dataset.parquet", columns=selected_movies_columns)
  end = time.perf_counter()

  sql_result = psql.sqldf("SELECT COUNT(*) FROM df_movies WHERE strftime('%m', release_date) = '{mes}'".format(mes=mes))
  cantidad_filmaciones_mes = sql_result.values[0][0]
  
  return {
    "message": "{cantidad_filmaciones_mes} películas fueron estrenadas en el mes de {mes}".format(cantidad_filmaciones_mes=cantidad_filmaciones_mes, mes=mes),
    "mes": mes,
    "cantidad_de_filmaciones": cantidad_filmaciones_mes.item(),
    "tiempo_lectura_parquet_movies": f"{end - start:0.4f} segundos",
    "tamaño_dataset_movies": f"{sys.getsizeof(df_movies)/1024/1024:0.4f} MB",
    "error": None
  }

#Se ingresa un día en idioma Español. Debe devolver la cantidad de películas 
#que fueron estrenadas en día consultado en la totalidad del dataset.
@app.get("/cantidad_filmaciones_dia")
async def cantidad_filmaciones_mes(dia: int):
  if dia < 1 or dia > 31:
    return {"dia": dia, "error": "Día no existe"}
  
  #transformar el mes a string de 2 digitos
  if dia < 10:
    dia = "0" + str(dia)
  else:
    dia = str(dia)
  
  selected_movies_columns = ['release_date'] 
  start = time.perf_counter()  
  df_movies = pd.read_parquet("datasets/movies_dataset.parquet", columns=selected_movies_columns)
  end = time.perf_counter()

  sql_result = psql.sqldf("SELECT COUNT(*) FROM df_movies WHERE strftime('%d', release_date) = '{dia}'".format(dia=dia))
  cantidad_filmaciones_dia = sql_result.values[0][0]
  
  return {
    "message": "{cantidad_filmaciones_dia} películas fueron estrenadas en el día {dia}".format(cantidad_filmaciones_dia=cantidad_filmaciones_dia, dia=dia),
    "dia": dia,
    "cantidad_de_filmaciones": cantidad_filmaciones_dia.item(),
    "tiempo_lectura_parquet_movies": f"{end - start:0.4f} segundos",
    "tamaño_dataset_movies": f"{sys.getsizeof(df_movies)/1024/1024:0.4f} MB",
    "error": None
  }

