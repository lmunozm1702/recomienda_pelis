#módulo de código reutilizado.
import time
import pandas as pd

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
    "domingo": 7
  }

  return dias.get(dia.lower(), None)

#"tiempo_lectura_parquet_credits": f"{credits_end - credits_start:0.4f} segundos",
#"tamaño_dataset_credits": f"{sys.getsizeof(df_credits)/1024/1024:0.4f} MB",
#"tiempo_lectura_parquet_movies": f"{movies_end - movies_start:0.4f} segundos",
#"tamaño_dataset_movies": f"{sys.getsizeof(df_movies)/1024/1024:0.4f} MB",