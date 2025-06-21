import json
import re
import pandas as pd
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import pandas as pd
from selenium.webdriver import Edge
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
import time
from datetime import datetime, timezone, timedelta
import re
import smtplib
import yaml
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.base import MIMEBase
from email import encoders
from loguru import logger
import numpy as np
import base64
from selenium.common.exceptions import TimeoutException
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from datetime import datetime, timezone
import psycopg2
import pandas as pd
import requests
from selenium import webdriver
from datetime import datetime
import json
import os

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import os

from helpers.db import get_engine
from helpers.db import get_connection  
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

variable = "CAUDAL"
variable_opcion = "C"


# ---------------------------- Funciones de apoyo ----------------------------

def generar_url(id_cuerpo_agua, fecha_hora):
    return f"https://www.senamhi.gob.pe/mapas/mapa-monitoreohidro/include/mnt-grafica-new.php?id={id_cuerpo_agua}&fecha_hora={fecha_hora.replace(' ', '%20')}&variable={variable}&variable_opcion={variable_opcion}"

def load_page_with_timeout(browser, url, timeout=20):
    browser.set_page_load_timeout(timeout)
    try:
        browser.get(url)
        wait = WebDriverWait(browser, timeout)
        wait.until(EC.presence_of_element_located(('tag name', 'body')))
    except Exception as e:
        print(f"Error al cargar {url}: {e}")
    return browser


def obtener_data(browser, id_cuerpo_agua, estacion, max_retries=3, retry_delay=5):
    """
    Obtiene los datos de una estación, con reintentos en caso de fallo.

    Args:
        browser: Instancia del navegador Selenium.
        id_cuerpo_agua: ID del cuerpo de agua.
        estacion: Nombre de la estación.
        max_retries: Número máximo de intentos de reintento.
        retry_delay: Tiempo en segundos entre reintentos.

    Returns:
        DataFrame con los datos si se obtienen con éxito, de lo contrario None.
    """
    for attempt in range(max_retries):
        fecha_hora = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        url = generar_url(id_cuerpo_agua, fecha_hora)

        print(f"[{estacion}] Intento {attempt + 1}/{max_retries} para obtener datos de: {url}")

        try:
            # Load the page and check if it was successful
            if not load_page_with_timeout(browser, url):
                if attempt < max_retries - 1:
                    print(f"[{estacion}] La página no se cargó correctamente. Reintentando en {retry_delay} segundos...")
                    time.sleep(retry_delay)
                continue # Go to the next attempt

            page_source = browser.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            script_tag = soup.select_one('body script:nth-child(4)')

            if script_tag and script_tag.string:
                script_content = script_tag.string
                data_match = re.search(r'dataCSV = \[.*?\]', script_content, re.DOTALL)

                if data_match:
                    data_content = data_match.group(0)
                    data_string = data_content[10:]
                    data_list = json.loads(data_string)
                    df = pd.DataFrame(data_list)
                    df['estacion'] = estacion
                    return df  # Successfully obtained data, exit the loop
                else:
                    print(f"[{estacion}] No se encontró la variable 'dataCSV' en el script.")
            else:
                print(f"[{estacion}] No se encontró el tag script o su contenido.")

        except Exception as e:
            print(f"[{estacion}] Error procesando datos en el intento {attempt + 1}: {e}")

        # If data not found or error, and it's not the last attempt, wait and retry
        if attempt < max_retries - 1:
            print(f"[{estacion}] Reintentando en {retry_delay} segundos...")
            time.sleep(retry_delay)
        else:
            print(f"[{estacion}] Fallaron todos los intentos. No se pudieron obtener datos después de {max_retries} intentos.")

    return None  # Return None if all attempts fail


# ---------------------------- Obtener estaciones ----------------------------
def fetch_estaciones():
    conn = get_connection()
    if conn is None:
        print("No se pudo conectar a la base de datos.")
        return []

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT EstacionID, Estacion FROM Estaciones WHERE Activa = TRUE;")
            return cur.fetchall()
    except Exception as e:
        print("Error al obtener datos de Estaciones:", e)
        return []
    finally:
        conn.close()

# def formatear_data(df, estacion, umbral_neutro=0):
#     # Formatear DataFrame
#     df = df.iloc[1:-1].copy()  # Elimina cabecera y última fila vacía si existe
#     df['fechaHora'] = df['fechaHora'].str.replace(' GMT', '')
#     df['fechaHora'] = pd.to_datetime(df['fechaHora'])
#     df['Fecha'] = df['fechaHora'].dt.date
#     df['Hora'] = df['fechaHora'].dt.time
#     df['Dato'] = df['dato']
#     df['Mes'] = df['fechaHora'].dt.strftime('%B')

#     # Umbral neutro constante
#     df['Umbral'] = umbral_neutro
#     df['Estado'] = 'Sin definir'
#     df['Estacion'] = estacion

#     # Eliminar columnas innecesarias (solo si existen)
#     columnas_a_eliminar = ['fechaHora', 'dato']
#     df = df.drop(columns=[col for col in columnas_a_eliminar if col in df.columns])

#     return df
def formatear_data(df, estacion, umbral_neutro=0):
    df = df.iloc[1:-1].copy()
    df['fechaHora'] = df['fechaHora'].str.replace(' GMT', '')
    df['fechaHora'] = pd.to_datetime(df['fechaHora'])
    df['Fecha'] = df['fechaHora'].dt.date
    df['Hora'] = df['fechaHora'].dt.time
    df['registro_ts'] = df['fechaHora']
    df['Dato'] = pd.to_numeric(df['dato'], errors='coerce')
    df['Mes'] = df['fechaHora'].dt.strftime('%B')
    df['Umbral'] = umbral_neutro
    df['Estado'] = 'Sin definir'
    df['Estacion'] = estacion
    columnas_a_eliminar = ['fechaHora', 'dato']
    df = df.drop(columns=[col for col in columnas_a_eliminar if col in df.columns])
    return df
def obtener_data_con_browser(id_cuerpo_agua, estacion):
    # Cada hilo crea su propio navegador
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    browser = webdriver.Chrome(options=chrome_options)

    return obtener_data(browser, id_cuerpo_agua, estacion)

def insertar_datos_nuevos(data_to_insert_df, estacion_id):
    try:
        conn = get_connection()
        if conn is None:
            print("No se pudo conectar a la base de datos.")
            return

        with conn.cursor() as cur:
            for _, row in data_to_insert_df.iterrows():
                cur.execute("""
                    INSERT INTO DatosSensor (EstacionID, Fecha, Hora, Valor, Estado, UmbralID, UmbralUsuarioID)
                    VALUES (%s, %s, %s, %s, %s, NULL, NULL);
                """, (
                    estacion_id,
                    row['Fecha'],
                    row['Hora'],
                    row['Dato'],
                    row['Estado']
                ))
        conn.commit()
        print(f"[{estacion_id}] Datos insertados exitosamente.")
    except Exception as e:
        print(f"[{estacion_id}] Error al insertar datos: {e}")
    finally:
        conn.close()


def obtener_data_actual_db(estacion_id):
    """
    Recupera el último registro de DatosSensor para una estación específica de la DB,
    utilizando el campo 'registro_ts' (TIMESTAMP WITHOUT TIME ZONE).
    Retorna un DataFrame con la última fila o un DataFrame vacío si no hay datos.
    """
    conn = None
    try:
        conn = get_connection()
        if conn is None:
            logger.error("No se pudo conectar a la base de datos.")
            return pd.DataFrame()

        # Seleccionamos el campo 'registro_ts' que es el timestamp completo.
        # También puedes seleccionar Fecha y Hora si los necesitas para otras operaciones
        # pero para la comparación principal, registro_ts es lo más directo.
        query = """
            SELECT registro_ts, fecha, hora, valor, estado
            FROM DatosSensor
            WHERE estacionid = %s
            ORDER BY registro_ts DESC
            LIMIT 1;
        """

        with conn.cursor() as cur:
            cur.execute(query, (estacion_id,))
            row = cur.fetchone()

        if row:
            # Los resultados de psycopg2.fetchone() son una tupla.
            # Convertimos a DataFrame, asegurándonos de que registro_ts es un datetime object.
            # Puedes ajustar qué columnas exactamente te devuelve si solo necesitas el timestamp.
            last_record = {
                'registro_ts': row[0], # datetime.datetime object
                'Fecha': row[1],      # date object
                'Hora': row[2],       # time object
                'Valor': row[3],
                'Estado': row[4]
            }
            return pd.DataFrame([last_record])
        else:
            logger.info(f"[{estacion_id}] Consulta no trajo resultados o estación sin datos previos.")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"[{estacion_id}] Error al obtener el último dato de la DB: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

# def obtener_data_actual_db(estacion_id):
#     try:
#         engine = get_engine()
#         if engine is None:
#             print("No se pudo crear el engine de SQLAlchemy.")
#             return pd.DataFrame()

#         query = """
#             SELECT *
#             FROM DatosSensor
#             WHERE EstacionID = :estacion_id
#             ORDER BY Fecha DESC, Hora DESC;
#         """
#         df = pd.read_sql(query, engine, params={"estacion_id": estacion_id})
#         if df.empty or not {'Fecha', 'Hora', 'Valor', 'Estado'}.issubset(df.columns):
#             print(f"[{estacion_id}] Consulta no trajo columnas esperadas o está vacía.")
#             return pd.DataFrame()
#         return df
#     except Exception as e:
#         print(f"[{estacion_id}] Error al obtener datos actuales de BD: {e}")
#         return pd.DataFrame()


def verificar_y_registrar(estacion_id, estacion_nombre, df_nuevo):
    df_actual = obtener_data_actual_db(estacion_id)
    if df_actual.empty:
        print(f"[{estacion_nombre}] No hay datos actuales, insertando todos los nuevos.")
        insertar_datos_nuevos(df_nuevo, estacion_id)
        return

    last_db_date = df_actual.iloc[0]['Fecha']
    last_db_time = df_actual.iloc[0]['Hora']
    last_db_dt = datetime.combine(last_db_date, last_db_time)

    last_new_date = df_nuevo.iloc[-1]['Fecha']
    last_new_time = df_nuevo.iloc[-1]['Hora']
    last_new_dt = datetime.combine(last_new_date, last_new_time)

    if last_new_dt > last_db_dt:
        print(f"[{estacion_nombre}] Se detectaron nuevos datos.")
        # Filtrar datos más recientes
        nuevos_datos = df_nuevo[
            (df_nuevo['Fecha'] > last_db_date) |
            ((df_nuevo['Fecha'] == last_db_date) & (df_nuevo['Hora'] > last_db_time))
        ]
        if not nuevos_datos.empty:
            insertar_datos_nuevos(nuevos_datos, estacion_id)
        else:
            print(f"[{estacion_nombre}] Ningún dato nuevo pasó el filtro.")
    else:
        print(f"[{estacion_nombre}] No hay datos nuevos.")


def identificar_datos_para_insercion(estacion_id, df_nuevo_senamhi_formateado):
    """
    Identifica los datos de SENAMHI que son realmente nuevos comparados con la DB,
    utilizando el campo 'registro_ts'.
    """
    df_ultima_lectura_db = obtener_data_actual_db(estacion_id)

    if df_ultima_lectura_db.empty:
        logger.info(f"[{estacion_id}] No hay datos previos en la DB, preparando todos los datos de SENAMHI para inserción.")
        # df_nuevo_senamhi_formateado ya tiene 'registro_ts' de formatear_data
        return df_nuevo_senamhi_formateado
    else:
        # Extraer el datetime de la última lectura en la DB desde la columna 'registro_ts'
        last_db_datetime = df_ultima_lectura_db.iloc[0]['registro_ts']

        # Filtrar datos de SENAMHI que son estrictamente más recientes
        # df_nuevo_senamhi_formateado ya tiene la columna 'registro_ts' de formatear_data
        nuevos_datos_filtrados = df_nuevo_senamhi_formateado[
            df_nuevo_senamhi_formateado['registro_ts'] > last_db_datetime
        ].copy()

        if not nuevos_datos_filtrados.empty:
            logger.info(f"[{estacion_id}] Se encontraron {len(nuevos_datos_filtrados)} nuevos registros de SENAMHI.")
            return nuevos_datos_filtrados
        else:
            logger.info(f"[{estacion_id}] No hay datos de SENAMHI más recientes que los de la base de datos.")
            return pd.DataFrame()

def insertar_datos_nuevos_a_db(data_to_insert_df, estacion_id):
    """
    Inserta un DataFrame de nue
    vos datos en la tabla DatosSensor.
    Asume que data_to_insert_df tiene las columnas 'Fecha', 'Hora', 'Dato', 'Estado',
    'Umbral' (mapeado a UmbralAplicado) y 'registro_ts'.
    """
    if data_to_insert_df.empty:
        logger.info(f"[{estacion_id}] No hay datos para insertar en DatosSensor.")
        return

    conn = None
    try:
        conn = get_connection()
        if conn is None:
            logger.error("No se pudo conectar a la base de datos.")
            return

        with conn.cursor() as cur:
            for _, row in data_to_insert_df.iterrows():
                # Asegúrate de que los tipos coincidan con las columnas de tu tabla
                cur.execute("""
                    INSERT INTO DatosSensor (estacionid, fecha, hora, valor, estado, umbralid, umbralusuarioid)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """, (
                    estacion_id,
                    row['Fecha'],       # Usar la columna de fecha (date)
                    row['Hora'],        # Usar la columna de hora (time)
                    row['Dato'],
                    row['Estado'],
                    # umbralid y umbralusuarioid son NULL en tu imagen, maneja esto como necesites
                    # Si tienes IDs reales, pásalos aquí. Si no, mantén NULL.
                    None, # umbralid
                    None  # umbralusuarioid
                ))
            conn.commit()
            logger.info(f"[{estacion_id}] {len(data_to_insert_df)} nuevos datos insertados en DatosSensor.")
    except Exception as e:
        logger.error(f"[{estacion_id}] Error al insertar datos en DatosSensor: {e}")
    finally:
        if conn:
            conn.close()


# ---------------------------- Main con ThreadPoolExecutor ----------------------------

def procesar_estacion_worker(estacion_info):
    """
    Función trabajadora para ThreadPoolExecutor.
    Cada hilo crea su propio navegador.
    """
    estacion_id, estacion_nombre = estacion_info

    # Cada hilo debe crear su propia instancia de navegador
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-software-rasterizer")
    # chrome_options.add_argument("--remote-debugging-port=9222") # May conflict with multiple instances, remove if issues
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-background-networking")

    browser_instance = None # Initialize to None
    try:
        browser_instance = webdriver.Chrome(options=chrome_options)
        logger.info(f"--- Procesando estación: {estacion_nombre} (ID: {estacion_id}) ---")

        # 1. Obtener datos de SENAMHI
        df_senamhi_raw = obtener_data(browser_instance, estacion_id, estacion_nombre)

        if df_senamhi_raw is None or df_senamhi_raw.empty:
            logger.warning(f"[{estacion_nombre}] No se pudieron obtener datos de SENAMHI o el DataFrame está vacío.")
            return None # Return None to indicate failure for this station

        # 2. Formatear los datos obtenidos
        df_formateado = formatear_data(df_senamhi_raw, estacion_nombre, umbral_neutro=0)

        # 3. Identificar solo los datos realmente nuevos (Deduplicación)
        df_datos_a_insertar = identificar_datos_para_insercion(estacion_id, df_formateado)

        # 4. Si hay nuevos datos, insertar en la DB
        if not df_datos_a_insertar.empty:
            insertar_datos_nuevos_a_db(df_datos_a_insertar, estacion_id)
            # Aquí podrías llamar a la función para generar capturas y enviar alertas
            # Generar capturas de pantalla (ej. para el último dato nuevo)
            # last_new_record = df_datos_a_insertar.iloc[-1]
            # (Llamar a tu función generar_capturas aquí, pasando los parámetros necesarios)
            # Insertar imágenes
            # (Llamar a tu función insertar_imagenes_a_db aquí)
            # Enviar alertas
            # (Llamar a tu función send_email_alert aquí)
        else:
            logger.info(f"[{estacion_nombre}] No se encontraron datos nuevos para insertar.")
        
        return f"[{estacion_nombre}] Procesamiento completado." # Indicate success
    
    except Exception as e:
        logger.error(f"Error al procesar la estación {estacion_nombre}: {e}")
        return f"[{estacion_nombre}] Error en el procesamiento."
    finally:
        if browser_instance: # Ensure browser_instance was created before quitting
            browser_instance.quit()


def main():
    logger.add("app.log", rotation="500 MB", level="INFO") # Configure logging

    estaciones = fetch_estaciones()
    if not estaciones:
        logger.warning("No hay estaciones activas para procesar.")
        return

    logger.info(f"Iniciando procesamiento para {len(estaciones)} estaciones.")

    # Usar ThreadPoolExecutor para procesar estaciones en paralelo
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit tasks, passing (EstacionID, EstacionNombre) as a tuple
        futures = {executor.submit(procesar_estacion_worker, (est[0], est[1])): est[1] for est in estaciones}

        for future in as_completed(futures):
            estacion_nombre = futures[future]
            try:
                result = future.result()
                logger.info(f"Resultado del procesamiento para {estacion_nombre}: {result}")
            except Exception as e:
                logger.error(f"Error en el futuro para la estación {estacion_nombre}: {e}")

    logger.info("Procesamiento de todas las estaciones completado.")

if __name__ == "__main__":
    main()
