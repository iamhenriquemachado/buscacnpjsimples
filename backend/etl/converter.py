import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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

def convert_csv_to_parquet():
    list_csv_files = list_all_csv_files()

    if not list_all_csv_files:
        return None
    
    with open('etl/extract/F.K03200$Z.D50913.PAISCSV', 'r') as f:
        content = f.read()
        print(content)

if __name__ == "__main__":
    convert_csv_to_parquet()