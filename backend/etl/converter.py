import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os 
import logging
import ftfy
import unicodedata
import duckdb
from pathlib import Path

logging.basicConfig(level=logging.INFO)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def create_parquet_dirs():
    """Cria o diretório de destino para os arquivos Parquet, se não existir."""
    parquet_dir = 'backend/etl/parquet'
    
    # Verifica se o diretório já existe
    if os.path.exists(parquet_dir):
        logging.info(f"✔️  O diretório '{parquet_dir}' já existe.")
        return parquet_dir
        
    # Tenta criar o diretório
    try:
        logging.info(f"⚙️  Criando o diretório '{parquet_dir}'...")
        os.makedirs(parquet_dir, exist_ok=True)
        
        # Verifica se a criação foi bem-sucedida
        if os.access(parquet_dir, os.W_OK):
            logging.info(f"✅ Diretório '{parquet_dir}' criado com sucesso.")
            return parquet_dir
    except Exception as e:
        logging.error(f"❌ Erro ao criar diretório '{parquet_dir}': {e}")
        return None


def clean_text(text):
    if isinstance(text, str):
        text_fixed = ftfy.fix_text(text)
        return unicodedata.normalize('NFC', text_fixed)
    return text


import os
import logging
import duckdb
from pathlib import Path

def convert_csv_to_parquet():
    try:
        extract_dir = 'backend/etl/extract/'
        parquet_dir = 'backend/etl/parquet/'
        
        # Criar diretório de destino se não existir
        Path(parquet_dir).mkdir(parents=True, exist_ok=True)
        
        converted_files = []
        
        # Criar conexão persistente com configurações otimizadas
        conn = duckdb.connect()
        
        # Otimizações de performance
        conn.execute("SET threads TO 8;")  
        conn.execute("SET memory_limit = '8GB';")  
        conn.execute("SET temp_directory = '/tmp/duckdb_temp';")  
        
        # Listar arquivos CSV uma vez
        csv_files = [f for f in os.listdir(extract_dir) if 'CSV' in f]
        total_files = len(csv_files)
        
        if not csv_files:
            logging.warning("⚠️ Nenhum arquivo CSV encontrado para converter.")
            return None
        
        logging.info(f"📁 Encontrados {total_files} arquivo(s) CSV para converter")
        
        for idx, filename in enumerate(csv_files, 1):
            csv_path = os.path.join(extract_dir, filename)
            parquet_name = filename.rsplit('.', 1)[0] + ".parquet"
            parquet_path = os.path.join(parquet_dir, parquet_name)
            
            logging.info(f"⚙️ [{idx}/{total_files}] Convertendo '{filename}' → '{parquet_name}'")
            
            # Query otimizada com hints de performance
            conn.execute(f"""
                COPY (
                    SELECT * FROM read_csv(
                        '{csv_path}',
                        delim=';',
                        header=FALSE,
                        encoding='CP1252',
                        quote='"',
                        ignore_errors=TRUE,
                        parallel=TRUE,
                        buffer_size=262144,
                        sample_size=500000
                    )
                )
                TO '{parquet_path}' (
                    FORMAT PARQUET,
                    COMPRESSION 'ZSTD',
                    ROW_GROUP_SIZE 122880
                );
            """)
            
            logging.info(f"✅ [{idx}/{total_files}] Arquivo '.parquet' convertido e salvo em '{parquet_path}'")
            converted_files.append(parquet_path)
        
        # Fechar conexão
        conn.close()
        
        if converted_files:
            logging.info(f"🎉 {len(converted_files)} arquivo(s) convertido(s) com sucesso!")
            return converted_files
        else:
            logging.warning("⚠️ Nenhum arquivo processado.")
            return None
                    
    except FileNotFoundError as e:
        logging.error(f"❌ Erro: Diretório não encontrado - {e}")
        return None
    except Exception as e:
        logging.error(f"❌ Erro ao converter arquivos: {e}")
        return None





# Executar programa 
if __name__ == "__main__":
     create_parquet_dirs()
     convert_csv_to_parquet()