# HENRY Modulo 7 - MLOps, Primer Proyecto Individual

El proyecto tiene 3 partes principales:

- ETL: Extracción, Transformación y Carga de los datos, de acuerdo a los requerimientos indicados en el [requerimiento detallado](https://github.com/soyHenry/fe-ct-pimlops2/blob/main/Readme.md). Esto con el objetivo de limpiar los datos de origen.
- EDA: Análisis exploratorio de los datos. Ya con los datos limpios se debe investigar las relaciones que hay entre las variables del dataset.
- API: Creación de una API. Debe considerar 7 endpoints, 6 de ellos de consulta de los datos originales y un 7mo que implemente un recomendador de películas a partir del título de una de ellas y retorne una lista de 5 recomendaciones.

## Proyecto desarrollado utilizando:

- Juniper Notebooks, para EDA y ETL.
- Python 3.11 para eldesarrollos del API.

## Sitio Web Real (Producción)

[Recomienda Pelis](https://recomienda-pelis.onrender.com/docs)

## Empecemos!

### Instalación

- Crear una carpeta en el directorio de trabajo.
- Abrir un terminal en esa carpeta y ejecutar:

```bash
 git clone git@github.com:lmunozm1702/recomienda_pelis.git #descarga del repositorio
 pip install -r requirements.txt #instalación de librerías
 uvicorn main:app --host 0.0.0.0 --port 80 #inicialización del servidor
```

### Utilización

- ETL y EDA: ejecutar los notebooks `etl-movies.ipynb`, `etl-credits.ipynb`, `eda_movies.ipynb` y `eda_credits.csv`, en ese orden, para generar los archivos que serán utilizados por el API.
- API: una vez en el [landing page](https://recomienda-pelis.onrender.com/docs), utilizar cada uno de los endpoints ingresando los parámetros de entrada especificados para cada uno de ellos. Haz click para ver la [demo](https://www.loom.com/share/58c64edf2e344e4b82cf42bec5e7bfba?sid=7bda364a-9b1c-42f0-9597-66142c6a9241).

#### Consideraciones:

- Los endpoints responderan con toda la data de los datasets originales, salvo el recomendador que por problemas de uso de memoria tuvo que ser disminuido en la cantidad de datos, eliminando del dataset todas las películas que no tienen como idioma original el inglés o que hayan sido liberadas `(release_date)` antes del 2015.
- Puedes utilizar para probar el endpoint de recomendaciones, los títulos: "a year and change","sparrows","full out","chalk it up","blood father", y seguir luego con las que el sistema te vaya recomendando.

### Repositorio GitHub

[recomienda-pelis](https://github.com/lmunozm1702/recomienda_pelis)

## Autor

### 👤 Luis Muñoz

- GitHub: [@lmunozm1702](https://github.com/lmunozm1702)
- Twitter: [@lmunozm](https://twitter.com/lmunozm)
- LinkedIn: [luis-munoz-manriquez](https://www.linkedin.com/in/luis-munoz-manriquez)

## 🤝 Contribuciones

Contribuciones y sugerencias son bienvenidas!

Por favor házmelas llegar utilizando la [página de issues](../../issues/).

## Muéstrame tu apoyo

Dame una ⭐️ si te gusta este proyecto.
