import flask
import functions_framework
from flask import jsonify
from google.auth import exceptions
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials


@functions_framework.http # Decorador para indicar que esta es una función de Cloud Function
def cotiz_usd(request):    
    # Importamos la librería requests para realizar solicitudes HTTP
    from requests.structures import CaseInsensitiveDict
    import requests
    from pprint import pprint
    # URL de extracción de datos del BCRA
    "https://estadisticasbcra.com/api/documentacion"
    # Definimos la URL de la API que proporciona la cotización del USD blue
    url_dolar_blue = "https://api.estadisticasbcra.com/usd"

    # Creamos un diccionario de encabezados (headers) para enviar en la solicitud
    headers_dolar_blue = CaseInsensitiveDict()

    # Indicamos que aceptamos una respuesta en formato JSON
    headers_dolar_blue["Accept"] = "application/json"  

    # El encabezado "Authorization" contiene un token de acceso para autenticar la solicitud en la API.
    headers_dolar_blue["Authorization"] = "BEARER eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MjM3NTM0NTEsInR5cGUiOiJleHRlcm5hbCIsInVzZXIiOiJlaG1ldHRsZXJAZ21haWwuY29tIn0.-sfZ14G0cjIcTauafz6pnhPnWtvHMuJjYuZqaSAws4NgBKQNFcs6Lmt1cJNUVwUuhek2F78krwTxBj2ofo8K2A"

    # Realizamos una solicitud GET a la URL de la API, incluyendo los encabezados
    dolar_blue = requests.get(url_dolar_blue, headers=headers_dolar_blue)

    # Imprimimos en formato JSON la respuesta obtenida de la API utilizando pprint
    # pprint(dolar_blue.json())
    # Definimos la URL de la API que proporciona la cotización del USD blue
    url_dolar_of_min = "https://api.estadisticasbcra.com/usd_of_minorista"

    # Creamos un diccionario de encabezados (headers) para enviar en la solicitud
    headers_dolar_of_min = CaseInsensitiveDict()

    # Indicamos que aceptamos una respuesta en formato JSON
    headers_dolar_of_min["Accept"] = "application/json"  

    # El encabezado "Authorization" contiene un token de acceso para autenticar la solicitud en la API.
    headers_dolar_of_min["Authorization"] = "BEARER eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MjM3NTM0NTEsInR5cGUiOiJleHRlcm5hbCIsInVzZXIiOiJlaG1ldHRsZXJAZ21haWwuY29tIn0.-sfZ14G0cjIcTauafz6pnhPnWtvHMuJjYuZqaSAws4NgBKQNFcs6Lmt1cJNUVwUuhek2F78krwTxBj2ofo8K2A"

    # Realizamos una solicitud GET a la URL de la API, incluyendo los encabezados
    dolar_of_min = requests.get(url_dolar_of_min, headers=headers_dolar_of_min)

    # Imprimimos en formato JSON la respuesta obtenida de la API utilizando pprint
    # pprint(dolar_of_min.json())
    # Definimos la URL de la API que proporciona eventos relevantes (ministros de economia y hechos económicos importantes)
    url_eventos_relevantes = "https://api.estadisticasbcra.com/milestones"

    # Creamos un diccionario de encabezados (headers) para enviar en la solicitud
    headers_ev = CaseInsensitiveDict()

    # Indicamos que aceptamos una respuesta en formato JSON
    headers_ev["Accept"] = "application/json"  

    # El encabezado "Authorization" contiene un token de acceso para autenticar la solicitud en la API.
    headers_ev["Authorization"] = "BEARER eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MjM3NTM0NTEsInR5cGUiOiJleHRlcm5hbCIsInVzZXIiOiJlaG1ldHRsZXJAZ21haWwuY29tIn0.-sfZ14G0cjIcTauafz6pnhPnWtvHMuJjYuZqaSAws4NgBKQNFcs6Lmt1cJNUVwUuhek2F78krwTxBj2ofo8K2A"

    # Realizamos una solicitud GET a la URL de la API, incluyendo los encabezados
    eventos_relevantes = requests.get(url_eventos_relevantes, headers=headers_ev)

    # Imprimimos en formato JSON la respuesta obtenida de la API utilizando pprint
    # pprint(eventos_relevantes.json())
    import json    # Importamos libreria json

    # convertimos json a cadena de texto
    dolar_blue_data = json.dumps(dolar_blue.json())
    dolar_of_min_data = json.dumps(dolar_of_min.json())
    eventos_rel_data = json.dumps(eventos_relevantes.json())
    import pandas as pd     # Importamos libreria Pandas

    # Pasamos las cadenas de texto a dataframe
    df_dolar_blue = pd.read_json(dolar_blue_data)
    df_dolar_of_min = pd.read_json(dolar_of_min_data)
    df_eventos_rel = pd.read_json(eventos_rel_data)
    # Imprimimos primeras filas de cada dataframe

    df_dolar_blue.head(3)
    df_dolar_blue.rename(columns={'d':'fecha', 'v':'cotiz_blue'}, inplace=True)     # Cambiamos nombre a las columnas
    df_dolar_blue.head(3)
    df_dolar_of_min.head(3)
    df_dolar_of_min.rename(columns={'d':'fecha', 'v':'cotiz_of_min'}, inplace=True)     # Cambiamos nombre a las columnas
    df_dolar_of_min.head(3)
    df_eventos_rel.head(3)
    df_eventos_rel.rename(columns={'d':'fecha', 'e':'evento', 't':'cargo'}, inplace=True)     # Cambiamos nombre a las columnas
    df_eventos_rel.head(3)
    print("N° de filas en el dataframe de dolar blue:", df_dolar_blue.shape[0])
    print("N° de filas en el dataframe de dolar of min:", df_dolar_of_min.shape[0])
    print("N° de filas en el dataframe de eventos rel:", df_eventos_rel.shape[0])
    df_blue_vs_of = pd.merge(df_dolar_blue, df_dolar_of_min, how='left', on='fecha')
    df_blue_vs_of.head()
    df_blue_vs_of_eventos = pd.merge(df_blue_vs_of, df_eventos_rel, how='left', on='fecha')
    df_blue_vs_of_eventos.head()
    import plotly.express as px     # Importamos la libreria plotly para realizar gráficos
    # Grafico de lineas con los valores del dolar blue
    fig = px.line(df_blue_vs_of_eventos, x='fecha', y='cotiz_blue')
    fig.show()
    # Grafico de lineas con los valores del dolar oficial minorista
    fig = px.line(df_blue_vs_of_eventos, x='fecha', y='cotiz_of_min')
    fig.show()
    # Graficamos las 2 cotizaciones juntas
    fig = px.line(df_blue_vs_of_eventos, x='fecha', y=['cotiz_blue', 'cotiz_of_min'],
                labels={'fecha': 'Fecha', 'value': 'Cotización'},
                color_discrete_map={'cotiz_blue': 'blue', 'cotiz_of_min': 'green'})
    fig.show()
    # Vamos a filtrar, dejando en el dataframe los registros desde el año 2010.
    df_blue_vs_of_eventos.head()
    # Convierte la columna 'fecha' al formato de fecha
    df_blue_vs_of_eventos['fecha'] = pd.to_datetime(df_blue_vs_of_eventos['fecha'])

    # Extrae el año y guárdalo en una nueva columna 'año'
    df_blue_vs_of_eventos['anio'] = df_blue_vs_of_eventos['fecha'].dt.year

    df_blue_vs_of_eventos.head()
    # Creamos un nuevo dataframe, filtrando desde el año 2010 a hoy
    df_cotiz_y_eventos = df_blue_vs_of_eventos[df_blue_vs_of_eventos['anio'] >= 2010]
    df_cotiz_y_eventos.head()
    # Graficamos las 2 cotizaciones juntas
    fig = px.line(df_cotiz_y_eventos, x='fecha', y=['cotiz_blue', 'cotiz_of_min'],
                labels={'fecha': 'Fecha', 'value': 'Cotización'},
                color_discrete_map={'cotiz_blue': 'blue', 'cotiz_of_min': 'green'})
    fig.show()
    # Agregamos una columna calculando la brecha cambiaria (dif entre valor oficial y blue)
    df_cotiz_y_eventos['brecha'] = ( ( df_cotiz_y_eventos['cotiz_blue'] / df_cotiz_y_eventos['cotiz_of_min'] ) - 1 ) * 100
    df_cotiz_y_eventos.head()
    # Graficamos con lineas, las variaciones de la brecha cambiaria en el tiempo
    fig = px.line(df_cotiz_y_eventos, x='fecha', y='brecha',
                labels={'fecha': 'Fecha', 'brecha': 'Brecha Cambiaria'},
                color_discrete_map={'brecha': 'violet'})
    fig.show()
    # Preguntas a responder en el analisis que pueden ser interesantes para incluir

    # ¿Qué eventos se relacionan con esos cambios?
    # Relacion entre emision monetaria (o crecimiento de la base monetaria) y el valor del dolar
    # Impacto del dolar en la inflacion, y tambien impacto de la emision monetaria en la inflación
    # Relacion entre el aumento de tasas de la fed y el valor del dolar

    # Unificamos las columnas evento y cargo
    df_cotiz_y_eventos['evento_n'] = df_cotiz_y_eventos['evento'] + ' - ' + df_cotiz_y_eventos['cargo']
    df_cotiz_y_eventos.head(10)
    import plotly.express as px
    import plotly.graph_objects as go

    # Gráfico con valores de dolar blue y oficial, y especificando los eventos que fueron ocurriendo.

    # Crear el gráfico de líneas usando Plotly Express
    fig = px.line(df_cotiz_y_eventos, x='fecha', y=['cotiz_blue', 'cotiz_of_min'],
                labels={'fecha': 'Fecha', 'value': 'Cotización'},
                color_discrete_map={'cotiz_blue': 'blue', 'cotiz_of_min': 'green'})

    # Agregar puntos de evento a las fechas correspondientes
    eventos = df_cotiz_y_eventos[df_cotiz_y_eventos['evento_n'].notna()]

    event_trace = go.Scatter(x=eventos['fecha'], y=eventos['cotiz_blue'],
                            mode='markers', name='Evento', marker=dict(color='red', size=10),
                            text=eventos['evento_n'])

    fig.add_trace(event_trace)
    fig.show()
    import plotly.express as px
    import plotly.graph_objects as go

    # Gráfico con valores de dolar blue y oficial, y especificando los eventos que fueron ocurriendo.

    # Crear el gráfico de líneas usando Plotly Express
    fig = px.line(df_cotiz_y_eventos, x='fecha', y='brecha',
                labels={'fecha': 'Fecha', 'brecha': 'Brecha Cambiaria (Dif en %  entre dolar blue y oficial)'},
                color_discrete_map={'brecha': 'violet'})

    # Agregar puntos de evento a las fechas correspondientes
    eventos = df_cotiz_y_eventos[df_cotiz_y_eventos['evento_n'].notna()]

    event_trace = go.Scatter(x=eventos['fecha'], y=eventos['brecha'],
                            mode='markers', name='Evento', marker=dict(color='red', size=10),
                            text=eventos['evento_n'])

    fig.add_trace(event_trace)
    fig.show()
    # Eliminar filas con valores nulos en 'cotiz_of_min', ya que esos dias no hubo cotización del dolar oficial minorista
    df_cotiz_y_eventos = df_cotiz_y_eventos.dropna(subset=['cotiz_of_min'])

    # Convertir objetos Timestamp en strings para evitar errores de compatibilidad con el archivo json.
    df_cotiz_y_eventos['fecha'] = df_cotiz_y_eventos['fecha'].apply(lambda x: x.strftime('%Y-%m-%d'))

    # Redondea los valores en la columna "Columna1" a 2 decimales
    df_cotiz_y_eventos['cotiz_blue'] = df_cotiz_y_eventos['cotiz_blue'].round(2)
    df_cotiz_y_eventos['cotiz_of_min'] = df_cotiz_y_eventos['cotiz_of_min'].round(2)
    df_cotiz_y_eventos['anio'] = df_cotiz_y_eventos['anio'].round(0)
    df_cotiz_y_eventos['brecha'] = df_cotiz_y_eventos['brecha'].round(2)

    # Eliminamos columna evento y cargo, ya que esos datos estan en la columna evento_n
    columnas_a_eliminar = ['evento', 'cargo']
    df_cotiz_y_eventos = df_cotiz_y_eventos.drop(columnas_a_eliminar, axis=1)

    # Llenar valores nulos en "evento_n" con la leyenda "sin dato"
    df_cotiz_y_eventos['evento_n'] = df_cotiz_y_eventos['evento_n'].fillna("Sin dato")
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials

    # Cargar tus datos en un DataFrame (reemplaza esto con tus datos)
    df = pd.DataFrame(df_cotiz_y_eventos)

    # Cargar las credenciales desde el archivo JSON descargado
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('basic-campus-272502-7a39c4c3aa7d.json', scope)
    client = gspread.authorize(creds)

    # Abre la hoja de cálculo por su título
    sheet = client.open('cotiz_usd')

    # Obtén la primera hoja de la hoja de cálculo
    worksheet = sheet.get_worksheet(0)

    # Agregar los nombres de las columnas en la primera fila
    column_names = df.columns.tolist()
    worksheet.update('A1', [column_names])

    # Borra los datos existentes en la hoja (excluyendo la primera fila)
    data_range = f"A2:Z{worksheet.row_count}"
    empty_data = [[''] * len(column_names)] * (worksheet.row_count - 1)
    worksheet.update(data_range, empty_data)

    # Convierte el DataFrame en una lista de listas para cargar en la hoja
    values = df.values.tolist()

    # Carga los datos en la hoja de cálculo
    worksheet.insert_rows(values, 2)  # Inserta después de la primera fila (encabezados)

    print("Datos exportados a Google Sheets")
    return jsonify({"message": "Función ejecutada exitosamente."})
