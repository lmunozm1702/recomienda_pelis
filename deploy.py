import sys
import time
import pandas as pd
import pandasql as psql

selected_movies_columns = ['belongs_to_collection', 'popularity', 'release_year', 'return']

start = time.perf_counter()
df_credits = pd.read_parquet("datasets/credits_dataset.parquet")
end = time.perf_counter()
print(f"Tiempo de lectura del parquet credits: {end - start:0.4f} segundos")

start = time.perf_counter()
df_movies = pd.read_parquet("datasets/movies_dataset.parquet", columns=selected_movies_columns)
end = time.perf_counter()
print(f"Tiempo de lectura del parquet movies: {end - start:0.4f} segundos\n")

print(f"tamaño dataframe credits: {sys.getsizeof(df_credits)/1024/1024} MB")
print(f"tamaño dataframe movies: {sys.getsizeof(df_movies)/1024/1024} MB\n")

print(df_credits.columns)
print("\n")
print(df_movies.columns)

print("\n\n*** using pandas query")
print(df_movies.query("release_year == 2015"))

print("\n\n*** using pandasql")
print(psql.sqldf("SELECT * FROM df_movies WHERE release_year = 2015"))