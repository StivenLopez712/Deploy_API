from fastapi import FastAPI
import pandas as pd
import numpy as np
app = FastAPI()

# Cargar los datos necesarios
tabla_funciones = pd.read_parquet('tabla_funciones.parquet')
F3 = pd.read_parquet('F3.parquet')
F4 = pd.read_parquet('F4.parquet')
F5 = pd.read_parquet('F5.parquet')
matrix_norm = pd.read_parquet('matrix_norm.parquet')
item_sim_df = pd.read_parquet('item_sim_df.parquet')

# Función 1
@app.get('/PlayTimeGenre')
def play_time_genre(genero: str):
    max_year = tabla_funciones[tabla_funciones['Genres'].str.contains(genero, case=False, na=False)]\
        .groupby('Release_Year')['Playtime_Forever'].sum().idxmax()
    result = {"Año de lanzamiento con más horas jugadas para Género " + genero: int(max_year)}
    return result
# Función 2
@app.get('/UserForGenre')
def user_for_genre(genero: str):
    dfgenero2 = tabla_funciones[tabla_funciones['Genres'].str.contains(genero, case=False, na=False)]
    user_time = dfgenero2.groupby('User_Id')['Playtime_Forever'].sum()
    maxtime = user_time.idxmax()
    playtime_year = dfgenero2.groupby('Release_Year')['Playtime_Forever'].sum().reset_index()
    result = {"Usuario con más horas jugadas para Género " + genero: maxtime,
              "Horas jugadas": playtime_year.to_dict(orient='records')}
    return result


# Función 3
@app.get('/UsersRecommend')
def users_recommend(año: int):
    merged_df = pd.merge(DF_GAMES, DF_REVIEWS, on='Item_Id')
    year_df = merged_df[merged_df['Year_Posted'] == año]
    positive_neutral_df = year_df[(year_df['Sentiment_Analysis'].isin([1, 2])) & (year_df['Recommend'])]
    top_games = positive_neutral_df.groupby('App_Name')['Recommend'].sum().reset_index()
    top_games = top_games.sort_values(by='Recommend', ascending=False).head(3)
    result_list = [{"Puesto {}: ".format(i+1): game} for i, game in enumerate(top_games['App_Name'])]
    return result_list

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