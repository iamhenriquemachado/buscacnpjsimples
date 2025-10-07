import psycopg2
import os
from dotenv import load_dotenv
import logging

load_dotenv()

DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASS = os.environ["DB_PASS"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = os.environ["DB_PORT"]


try:
    conn = psycopg2.connect(        
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT)
    print("✅ Conexão com o PostgreSQL bem-sucedida!")

except psycopg2.OperationalError as e:
    print(f"❌ Erro ao conectar ao PostgreSQL: {e}")
finally:
    if conn:
        conn.close()
        print("🔌 Conexão com o PostgreSQL fechada.")


def get_connection():
    try:
        return psycopg2.connect(cbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT, 
        timeout=5)
    except psycopg2.Error as ex:
        sqlstate = ex.args[0]
        logging.error(f"Erro de conexão com o banco de dados: {sqlstate}")
        raise

def fetch_one(query: str, params: tuple = None):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params if params else [])
                return cursor.fetchone()
    except psycopg2.Error as e:
        logging.error(f"Erro ao executar fetch_one: {e}")
        return None

def fetch_all(query: str, params: tuple = None):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params if params else [])
                return cursor.fetchall()
    except psycopg2.Error as e:
        logging.error(f"Erro ao executar fetch_all: {e}")
        return []

def execute_commit(query: str, params: tuple = None):
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params if params else [])
                conn.commit()
                return cursor.rowcount 
    except psycopg2.Error as e:
        logging.error(f"Erro ao executar execute_commit: {e}")
        return None