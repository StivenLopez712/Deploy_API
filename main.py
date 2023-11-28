from fastapi import FastAPI
import pandas as pd
import numpy as np
app = FastAPI()

# Cargar los datos necesarios
tabla_funciones = pd.read_parquet('tabla_funciones.parquet')
F3 = pd.read_parquet('/Parquets/F3.parquet')
F4 = pd.read_parquet('/Parquets/F4.parquet')
F5 = pd.read_parquet('/Parquets/F5.parquet')
matrix_norm = pd.read_parquet('/Parquets/matrix_norm.parquet')
item_sim_df = pd.read_parquet('/Parquets/item_sim_df.parquet')

# Función 1
@app.get('/PlayTimeGenre')
def play_time_genre(genero: str):
    max_year = tabla_funciones[tabla_funciones['Genres'].str.contains(genero, case=False, na=False)]\
        .groupby('Release_Year')['Playtime_Forever'].sum().idxmax()
    result = {"Año de lanzamiento con más horas jugadas para Género " + genero: int(max_year)}
    return result
# Función 5
@app.get('/sentiment_analysis')
def sentiment_analysis(desarrolladora: str):
    sentiment_counts = F5['Sentiment_Analysis'].value_counts().to_dict()
    output_dict = {desarrolladora: {'Negative': sentiment_counts.get(0, 0),
                                    'Neutral': sentiment_counts.get(1, 0),
                                    'Positive': sentiment_counts.get(2, 0)}}
    return output_dict

# Función 6
@app.get('/recomendacion_juego')
def recomendacion_juego(id_producto):
    count = 1
    result = {'mensaje': 'Similar games include:', 'juegos_recomendados': []}
    for item in item_sim_df.sort_values(by=id_producto, ascending=False).index[1:6]:
        result['juegos_recomendados'].append(''.join(item))
        count += 1
    return result