#módulo de código reutilizado.
import time
import pandas as pd

def read_movies_dataset(selected_movies_columns):
  start = time.perf_counter()  
  df_movies = pd.read_parquet("datasets/movies_dataset.parquet", columns=selected_movies_columns)
  end = time.perf_counter()

  return df_movies

def read_credits_dataset(selected_credits_columns):
  credits_start = time.perf_counter()
  df_credits = pd.read_parquet("datasets/credits_dataset.parquet", columns=selected_credits_columns)
  credits_end = time.perf_counter()

  return df_credits

#"tiempo_lectura_parquet_credits": f"{credits_end - credits_start:0.4f} segundos",
#"tamaño_dataset_credits": f"{sys.getsizeof(df_credits)/1024/1024:0.4f} MB",
#"tiempo_lectura_parquet_movies": f"{movies_end - movies_start:0.4f} segundos",
#"tamaño_dataset_movies": f"{sys.getsizeof(df_movies)/1024/1024:0.4f} MB",