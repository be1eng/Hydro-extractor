import os
import psycopg2
from dotenv import load_dotenv
from psycopg2 import OperationalError
from sqlalchemy import create_engine

load_dotenv()

def get_connection():
    try:
        use_socket = os.environ.get("USE_CLOUDSQL_SOCKET", "false").lower() == "true"

        if use_socket:
            socket_path = f"/cloudsql/{os.environ['INSTANCE_CONNECTION_NAME']}"
            conn = psycopg2.connect(
                dbname=os.environ["DB_NAME"],
                user=os.environ["DB_USER"],
                password=os.environ["DB_PASSWORD"],
                host=socket_path
            )
        else:
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
        use_socket = os.environ.get("USE_CLOUDSQL_SOCKET", "false").lower() == "true"

        if use_socket:
            socket_path = f"/cloudsql/{os.environ['INSTANCE_CONNECTION_NAME']}"
            db_url = (
                f"postgresql+psycopg2://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
                f"@/{os.environ['DB_NAME']}?host={socket_path}"
            )
        else:
            db_url = (
                f"postgresql+psycopg2://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
                f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
            )

        return create_engine(db_url)
    except Exception as e:
        print("Error al crear engine de SQLAlchemy:", e)
        return None
