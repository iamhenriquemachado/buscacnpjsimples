import pandas as pd
import pyarrow
import os 
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def list_all_csv_files():
    file_list = []
    file_dir = os.listdir('etl/extract')

    file_list.append(file_dir)
    list_formatted = [item for sublist in file_list for item in sublist]

    return list_formatted

def create_parquet_dirs():

    parquet_dir = 'etl/parquet'
    if os.path.exists(parquet_dir):
        logging.info(f"⚠️ O diretório {parquet_dir} já existe.")
        return parquet_dir
    try:
        logging.info(f"📂 Criando o diretório '{parquet_dir}'.")
        os.makedirs(parquet_dir, exist_ok=True)
    

        if os.access(parquet_dir, os.W_OK):
            logging.info(f"✅ Diretório '{parquet_dir}' criado com sucesso.")
            return parquet_dir
    except Exception as e:
        logging.info(f"❌ Erro ao criar diretório {parquet_dir}")
        return None

if __name__ == "__main__":
    create_parquet_dirs()