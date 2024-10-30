import sys
import pandas as pd
import pandasql as psql
import json
import ast

from fastapi import FastAPI
from funciones import *
from datetime import datetime

app = FastAPI()

@app.get("/")
async def root():
  return {"message": "Hello World"}

#Se ingresa un mes en idioma Español. Debe devolver la cantidad de películas que fueron estrenadas en el mes 
#consultado en la totalidad del dataset.
#http://localhost:8000/cantidad_filmaciones_mes?mes=02
@app.get("/cantidad_filmaciones_mes")
async def cantidad_filmaciones_mes(mes: str):
  mes_numerico = mes_a_numeros(mes)
  if mes_numerico == None:
    return {"mes": mes, "error": "Mes no existe"}
    
  selected_movies_columns = ['release_date'] 
  df_movies = read_movies_dataset(selected_movies_columns)

  sql_result = psql.sqldf("SELECT COUNT(*) FROM df_movies WHERE strftime('%m', release_date) = '{mes}'".format(mes=mes_numerico))
  if sql_result.empty:
    return {"error": "No se encontraron resultados para el mes {mes}".format(mes=mes),
            "dia": mes,
          }
  
  cantidad_filmaciones_mes = sql_result.values[0][0]
  
  return {
    "message": "{cantidad_filmaciones_mes} películas fueron estrenadas en el mes de {mes}".format(cantidad_filmaciones_mes=cantidad_filmaciones_mes, mes=mes),
    "mes": mes,
    "cantidad_de_filmaciones": cantidad_filmaciones_mes.item(),
    "error": None
  }

#Se ingresa un día en idioma Español. Debe devolver la cantidad de películas 
#que fueron estrenadas en día consultado en la totalidad del dataset.
#http://localhost:8000/cantidad_filmaciones_dia?dia=2
@app.get("/cantidad_filmaciones_dia")
async def cantidad_filmaciones_dia(dia: int):
  if dia < 1 or dia > 31:
    return {"dia": dia, "error": "Día no existe"}
  
  #transformar el mes a string de 2 digitos
  if dia < 10:
    dia = "0" + str(dia)
  else:
    dia = str(dia)
  
  selected_movies_columns = ['release_date'] 
  df_movies = read_movies_dataset(selected_movies_columns)

  sql_result = psql.sqldf("SELECT COUNT(*) FROM df_movies WHERE strftime('%d', release_date) = '{dia}'".format(dia=dia))
  if sql_result.empty:
    return {"error": "No se encontraron resultados para el día {dia}".format(dia=dia),
            "dia": dia,
          }

  cantidad_filmaciones_dia = sql_result.values[0][0]
  
  return {
    "message": "{cantidad_filmaciones_dia} películas fueron estrenadas en el día {dia}".format(cantidad_filmaciones_dia=cantidad_filmaciones_dia, dia=dia),
    "dia": dia,
    "cantidad_de_filmaciones": cantidad_filmaciones_dia.item(),
    "error": None
  }

#Se ingresa el título de una filmación esperando como respuesta el título, el año de estreno y el score.
#http://localhost:8000/score_titulo?titulo=toy%20story
@app.get("/score_titulo")
async def score_titulo(titulo: str):
  if titulo == "":
    return {"titulo": titulo, "error": "Título no existe"}

  selected_movies_columns = ['title', 'release_year', 'popularity']  
  df_movies = read_movies_dataset(selected_movies_columns)
  
  sql_result = psql.sqldf("SELECT title, release_year, popularity FROM df_movies WHERE LOWER(title) = '{titulo}'".format(titulo=titulo.lower()))
  if sql_result.empty:
    return {
      "error": "La película {titulo} no existe".format(titulo=titulo),
    }
  
  titulo = sql_result.values[0][0]
  release_year = sql_result.values[0][1]
  score = sql_result.values[0][2]
  
  return {
    "message": "La película {titulo} fue estrenada en el año {release_year} con un score/popularidad de {score}".format(titulo=titulo, release_year=release_year, score=score),      
    "titulo": titulo,
    "release_year": release_year,
    "score": score,
    "error": None
  }

  #Se ingresa el título de una filmación esperando como respuesta el título, la cantidad de votos y el valor promedio de las votaciones. 
  # La misma variable deberá de contar con al menos 2000 valoraciones, caso contrario, debemos contar con un mensaje avisando que no cumple
  # esta condición y que por ende, no se devuelve ningun valor.
  #http://localhost:8000/votos_titulo?titulo=toy%20story

@app.get("/votos_titulo")
async def votos_titulo( titulo: str):
  if titulo == "":
    return {"titulo": titulo, "error": "Título no existe"}
  selected_movies_columns = ['title', 'vote_count', 'vote_average', 'release_year'] 
  
  df_movies = read_movies_dataset(selected_movies_columns)

  sql_result = psql.sqldf("SELECT title, vote_count, vote_average, release_year FROM df_movies WHERE LOWER(title) = '{titulo}'".format(titulo=titulo.lower()))
  if sql_result.empty:
    return {
      "error": "La película {titulo} no existe".format(titulo=titulo),
    }
  
  titulo = sql_result.values[0][0]
  vote_count = sql_result.values[0][1]
  vote_average = sql_result.values[0][2]
  release_year = sql_result.values[0][3]

  if vote_count < 2000:
    return {
      "message": "La película {titulo} no cuenta con suficientes votos para entregar un resultado".format(titulo=titulo),
      "vote_count" : vote_count,
    }
  
  return {
    "message": "La película {titulo} fue estrenada en el año {release_year}. Cuenta con un total de {vote_count} valoraciones y un promedio de {vote_average}".format(titulo=titulo, vote_count=vote_count, vote_average=vote_average, release_year=release_year),      
    "titulo": titulo,
    "vote_count": vote_count,
    "vote_average": vote_average,
    "release_year": release_year,
    "error": None
  }
    
#Se ingresa el nombre de un actor que se encuentre dentro de un dataset debiendo devolver el éxito del mismo medido a través del retorno. 
#Además, la cantidad de películas que en las que ha participado y el promedio de retorno. La definición no deberá considerar directores.
#http://localhost:8000/get_actor?actor=KEVIN%20COSTNER
@app.get("/get_actor")
async def get_actor( actor: str ):
  if actor == "":
    return {"actor": actor, "error": "Actor no existe"}
  selected_movies_columns = ['return', 'id'] 
  selected_credits_columns = ['id', 'actors']
  

  df_credits = pd.read_parquet("datasets/actores_dataset.parquet")
  df_movies = read_movies_dataset(selected_movies_columns)

  sql_result = psql.sqldf("SELECT count(df_movies.id) as cantidad, SUM(return) as retorno_total FROM df_movies join df_credits on df_movies.id = df_credits.id WHERE LOWER(df_credits.name) = '{actor}'".format(actor=actor.lower()))
  
  if sql_result.empty or sql_result.values[0][0] == 0 or sql_result.values[0][1] == None:
    return {
      "error": "El actor {actor} no existe".format(actor=actor),
    }
  
  cantidad = (sql_result.values[0][0])
  retorno_total = (sql_result.values[0][1]).round(1)
  retorno_promedio = (retorno_total / cantidad).round(1)

  return {
    "message": "El actor {actor} ha participado de {cantidad} películas y ha conseguido un retorno total de {retorno_total}, con un promedio de {retorno_promedio}".format(actor=actor, cantidad=cantidad, retorno_total=retorno_total, retorno_promedio=retorno_promedio),
    "actor": actor,
    "cantidad": cantidad,
    "retorno_total": retorno_total,
    "retorno_promedio": retorno_promedio,
    "error": None
  }

#Se ingresa el nombre de un director que se encuentre dentro de un dataset debiendo devolver el éxito del mismo medido a través del retorno. 
#Además, deberá devolver el nombre de cada película con la fecha de lanzamiento, retorno individual, costo y ganancia de la misma.
#http://localhost:8000/get_director?director=STEVEN%20SPIELBERG

@app.get("/get_director")
async def get_director( director ):
  if director == "":
    return {"director": director, "error": "Director no existe"}
  
  selected_movies_columns = ['title', 'release_date', 'return', 'budget', 'revenue', 'id']

  df_credits = pd.read_parquet("datasets/directores_dataset.parquet")
  df_movies = read_movies_dataset(selected_movies_columns)  

  sql_result = psql.sqldf("SELECT title, release_date, return, budget, revenue FROM df_movies join df_credits on df_movies.id = df_credits.id WHERE LOWER(df_credits.name) = '{director}'".format(director=director.lower()))
  retorno_total = sql_result['return'].sum()

  if sql_result.empty:
    return {
      "error": "El director {director} no existe".format(director=director),
    }

  return {
    "director": director,
    "retorno": retorno_total,
    "peliculas": [
      {
        "titulo": row[0],
        "fecha de lanzamiento": datetime.strptime(row[1], "%Y-%m-%d").strftime("%d-%m-%Y"),
        "retorno pelicula": round(row[2],1),
        "costo": int(row[3]),
        "ganancia": row[4]
      } for row in sql_result.itertuples(index=False)
    ],
    "error": None
  }