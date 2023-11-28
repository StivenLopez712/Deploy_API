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


@app.get('/')
def read_root():
    """¡ Bienvenidos a API STEAM GAMES!
    Aquí encontrarás diferentes funciones que proporcionan información 
    y te permiten realizar consultas simplemente dando click en 'Try Out' en cada una, 
    la descripcion de cada función y sus parametros está junto a cada sección.
    """
    return {"mensaje": "¡Bienvenido a mi API FastAPI!"}


# Función 1
@app.get('/PlayTimeGenre')

def play_time_genre(genero: str):
    '''Esta funcion devuelve el año de lanzamiento con más horas 
    jugadas para un género específico.
    
    Parámetros de entrada:
    - género (cadena de texto), por ejemplo Action, Casual, RPG, Strategy, etc.
    '''
    max_year = tabla_funciones[tabla_funciones['Genres'].str.contains(genero, case=False, na=False)]\
        .groupby('Release_Year')['Playtime_Forever'].sum().idxmax()
    result = {"Año de lanzamiento con más horas jugadas para Género " + genero: int(max_year)}
    return result
# Función 2
@app.get('/UserForGenre')

def user_for_genre(genero: str):
    '''Esta funcion devuelve el usuario con más horas 
       jugadas para un género específico.
    
    Parámetros de entrada:
    - género (cadena de texto), por ejemplo Action, Casual, RPG, Strategy, etc.
    '''
    genre_data = tabla_funciones[tabla_funciones['Genres'].str.contains(genero, case=False, na=False)]
    user_time = genre_data.groupby('User_Id')['Playtime_Forever'].sum()
    max_user_time = user_time.idxmax()
    playtime_by_year = genre_data.groupby('Release_Year')['Playtime_Forever'].sum().reset_index()
    result = {
       ' El usuario con mas hras es': max_user_time,
       ' con un total de ': playtime_by_year.to_dict('records')
    }
    return result


# Función 3
@app.get('/UsersRecommend')
def UsersRecommend(año: int):
    '''Esta funcion devuelve el top 3 de juegos más recomendados 
       por los usuarios en un año dado.
    
    Parámetros de entrada:
    - Año (número entero), esta funcion solo retorna la lista para los años 2010 a 2015'''
    df_filtered = F3[(F3['Year_Posted'] == año) & (F3['Recommend'] == True) & (F3['Sentiment_Analysis'] >= 1)]
    game_counts = df_filtered['Item_Name'].value_counts()
    top_3 = game_counts[:3].index.tolist()
    result = [{"Puesto 1": top_3[0]}, {"Puesto 2": top_3[1]}, {"Puesto 3": top_3[2]}]
    
    return result

#Funcion 4
@app.get('/WorstDeveloper')
def UsersWorstDeveloper(año: int):
    '''Esta funcion devuelve el top 3 de peores desarroladores  
       según los usuarios en un año dado.
    
    Parámetros de entrada:
    - Año (número entero), esta funcion solo retorna la lista para los años 2010 a 2015'''
    df_filtered = F4[(F4['Year_Posted'] == año) & (F4['Recommend'] == False) & (F4['Sentiment_Analysis'] == 0)]
    developer_counts = df_filtered['Developer'].value_counts()
    worst_dev = developer_counts[:3].index.tolist()
    result = []
    if len(worst_dev) > 0: result.append({"Puesto 1": worst_dev[0]})
    if len(worst_dev) > 1: result.append({"Puesto 2": worst_dev[1]})
    if len(worst_dev) > 2: result.append({"Puesto 3": worst_dev[2]})
    
    return result

# Función 5
@app.get('/sentiment_analysis')
def sentiment_analysis(desarrolladora: str):
    '''Devuelve la cantidad de reseñas Negativas (Negative), Neutras (Neutral) y Positivas (Positive)
    para un desarrolador dado.
    Parámetros de entrada:
    - Desarrolladora: Es la empresa encargada de desarrolar videojuegos, 
    por ejemplo: Valve, ActiVision, SEGA, etc'''
    sentiment_counts = F5['Sentiment_Analysis'].value_counts().to_dict()
    output_dict = {desarrolladora: {'Negative': sentiment_counts.get(0, 0),
                                    'Neutral': sentiment_counts.get(1, 0),
                                    'Positive': sentiment_counts.get(2, 0)}}
    return output_dict

# Función 6
@app.get('/recomendacion_juego')
def recomendacion_juego(id_producto):
    ''' Este modelo de recomendacion, toma un Videojuego, 
        y nos devuelve 5 juegos similares que podrían gustar al consumidor.
    Parámetros de entrada:
    - id_producto: Corresponde al nombre del videojuego como cadena de Texto.
    '''
    count = 1
    result = {'juegos_recomendados': []}
    for item in item_sim_df.sort_values(by=id_producto, ascending=False).index[1:6]:
        result['juegos_recomendados'].append(''.join(item))
        count += 1
    return result