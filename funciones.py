#módulo de código reutilizado.
import time
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

#Carga dataset de películas
def read_movies_dataset(selected_movies_columns):
  start = time.perf_counter()  
  df_movies = pd.read_parquet("datasets/movies_dataset.parquet", columns=selected_movies_columns)
  end = time.perf_counter()

  return df_movies

#Carga dataset de créditos
def read_credits_dataset(selected_credits_columns):
  credits_start = time.perf_counter()
  df_credits = pd.read_parquet("datasets/credits_dataset.parquet", columns=selected_credits_columns)
  credits_end = time.perf_counter()

  return df_credits

#Convierte el mes recibido en forma te texto (ej: "enero") a su correspondiente número (ej: "01")
def mes_a_numeros(mes):
  meses = {
    "enero": "01",
    "febrero": "02",
    "marzo": "03",
    "abril": "04",
    "mayo": "05",
    "junio": "06",
    "julio": "07",
    "agosto": "08",
    "septiembre": "09",
    "octubre": "10",
    "noviembre": "11",
    "diciembre": "12"
  }

  return meses.get(mes.lower(), None)

def dia_a_numeros(dia):
  dias = {
    "lunes": 1,
    "martes": 2,
    "miercoles": 3,
    "miércoles": 3,
    "jueves": 4,
    "viernes": 5,
    "sabado": 6,
    "sábado": 6,
    "domingo": 0
  }

  return dias.get(dia.lower(), None)

#"tiempo_lectura_parquet_credits": f"{credits_end - credits_start:0.4f} segundos",
#"tamaño_dataset_credits": f"{sys.getsizeof(df_credits)/1024/1024:0.4f} MB",
#"tiempo_lectura_parquet_movies": f"{movies_end - movies_start:0.4f} segundos",
#"tamaño_dataset_movies": f"{sys.getsizeof(df_movies)/1024/1024:0.4f} MB",

def carga_matriz_recomendaciones(data_recomendaciones):
  #remover conectores en inglés "the, in, on, at, with, for", etc.
  tfidf = TfidfVectorizer(stop_words='english')

  #crea matriz de vectores tfidf (Term Frequency - Inverse Document Frequency)
  #es la frecuencia de aparición de cada palabra en el overview de la película
  #ponderada por los overviews en los que aparece.
  tfidf_matrix = tfidf.fit_transform(data_recomendaciones['overview'])

  #calcula la similitud coseno entre las películas
  similitud_coseno = cosine_similarity(tfidf_matrix, tfidf_matrix)
  
  return similitud_coseno

def busca_recomendaciones(titulo):
  data_recomendaciones = pd.read_parquet('datasets/data_recomendaciones_dataset.parquet')
  
  #carga la matriz de recomendaciones
  similitud_coseno = carga_matriz_recomendaciones(data_recomendaciones)

  #busca el indice de la pelicula
  try:
    indice_titulo = data_recomendaciones['title'].str.contains(titulo).to_list().index(True)
  except:
    return []
  
  if indice_titulo == -1:
    return None

  #busca la fila que corresponde al puntaje de la película buscada
  puntajes = similitud_coseno[indice_titulo]
  
  #busca los 10 mejores puntajes ordenados de mayor a menor
  peliculas_similares = sorted(list(enumerate(puntajes)), reverse=True, key=lambda x: x[1])[1:11]
  
  #busca los indices en el dataset de los 10 mejores puntajes
  indices_recomendadas = [p[0] for p in peliculas_similares]

  #retorna los nombres de las peliculas recomendadas.
  return list(data_recomendaciones.iloc[indices_recomendadas]['title'])




