# db.py
import os
import psycopg2
from dotenv import load_dotenv
from psycopg2 import OperationalError
from sqlalchemy import create_engine

load_dotenv()

def get_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        return conn
    except OperationalError as e:
        print("Error de conexión a la base de datos:", e)
        return None

def get_engine():
    try:
        db_url = (
            f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
        return create_engine(db_url)
    except Exception as e:
        print("Error al crear engine de SQLAlchemy:", e)
        return None
