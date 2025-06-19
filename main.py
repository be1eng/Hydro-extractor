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

# ------------------------------------------Configuración de Chrome------------------------------------------
chrome_options = Options()
chrome_options.add_argument("--headless=new")  # Usa el nuevo modo headless (desde Chrome 109+)
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1920,1080")

# Inicia el navegador en segundo plano
browser = webdriver.Chrome(options=chrome_options)

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


def obtener_data(browser, id_cuerpo_agua, estacion):
    fecha_hora = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    url = generar_url(id_cuerpo_agua, fecha_hora)

    try:
        browser = load_page_with_timeout(browser, url)
        page_source = browser.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        script_content = soup.select_one('body script:nth-child(4)').string
        data_match = re.search(r'dataCSV = \[.*?\]', script_content, re.DOTALL)
        if data_match:
            data_content = data_match.group(0)
            data_string = data_content[10:]
            data_list = json.loads(data_string)
            df = pd.DataFrame(data_list)
            df['estacion'] = estacion
            return df
        else:
            print(f"[{estacion}] No se encontró la variable 'data'")
            return None
    except Exception as e:
        print(f"[{estacion}] Error procesando datos: {e}")
        return None
    finally:
        browser.quit()

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

def formatear_data(df, estacion, umbral_neutro=0):
    # Formatear DataFrame
    df = df.iloc[1:-1].copy()  # Elimina cabecera y última fila vacía si existe
    df['fechaHora'] = df['fechaHora'].str.replace(' GMT', '')
    df['fechaHora'] = pd.to_datetime(df['fechaHora'])
    df['Fecha'] = df['fechaHora'].dt.date
    df['Hora'] = df['fechaHora'].dt.time
    df['Dato'] = df['dato']
    df['Mes'] = df['fechaHora'].dt.strftime('%B')
    
    # Umbral neutro constante
    df['Umbral'] = umbral_neutro
    df['Estado'] = 'Sin definir'
    df['Estacion'] = estacion

    # Eliminar columnas innecesarias (solo si existen)
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
    try:
        engine = get_engine()
        query = """
            SELECT Fecha, Hora, Valor AS Dato, Estado
            FROM DatosSensor
            WHERE EstacionID = %s
            ORDER BY Fecha DESC, Hora DESC
            LIMIT 10;
        """
        df = pd.read_sql(query, engine, params=(estacion_id,))
        if df.empty or not {'Fecha', 'Hora', 'Dato', 'Estado'}.issubset(df.columns):
            print(f"[{estacion_id}] Consulta no trajo columnas esperadas o está vacía.")
            return pd.DataFrame()
        return df
    except Exception as e:
        print(f"[{estacion_id}] Error al obtener datos actuales de BD: {e}")
        return pd.DataFrame()




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


# ---------------------------- Main con ThreadPoolExecutor ----------------------------

def main():
    estaciones = fetch_estaciones()
    if not estaciones:
        print("No hay estaciones.")
        return

    formatted_results = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        # Lanza las tareas paralelamente, pasando ID y nombre de la estación
        futures = {
            executor.submit(obtener_data_con_browser, est[0], est[1]): est[1] for est in estaciones
        }


        for future in as_completed(futures):
            estacion_nombre = futures[future]  # Recupera el nombre de la estación asociada
            df = future.result()
            if df is not None and not df.empty:
                try:
                    # Aplica formateo con umbral neutro (ej. 0)
                    df_formateado = formatear_data(df, estacion_nombre, umbral_neutro=0)
                    formatted_results.append(df_formateado)
                except Exception as e:
                    print(f"[{estacion_nombre}] Error al formatear data: {e}")

    if formatted_results:
        df_final = pd.concat(formatted_results, ignore_index=True)
        print("Datos obtenidos y formateados.")
        print(df_final.head())

        for est_id, est_nombre in estaciones:
            df_est = df_final[df_final["Estacion"] == est_nombre]
            if not df_est.empty:
                verificar_y_registrar(est_id, est_nombre, df_est)

    else:
        print("No se recuperaron datos.")


if __name__ == "__main__":
    main()