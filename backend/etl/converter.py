import os
import logging
import ftfy
import unicodedata
import duckdb
import re
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def create_parquet_dirs():
    parquet_dir = 'backend/etl/parquet'
    try:
        Path(parquet_dir).mkdir(parents=True, exist_ok=True)
        logging.info(f"📂 Diretório '{parquet_dir}' verificado/criado com sucesso.")
        return parquet_dir
    except Exception as e:
        logging.error(f"❌ Erro ao criar diretório '{parquet_dir}': {e}")
        return None


def clean_text(text):
    if isinstance(text, str):
        text_fixed = ftfy.fix_text(text)
        return unicodedata.normalize('NFC', text_fixed)
    return text


def sanitize_filename(filename):
    """
    Renomeia o arquivo CSV de entrada para um nome Parquet limpo e padronizado.
    Ex: 'K3241.K03200Y1.D50913.EMPRECSV' -> 'Empresas1.parquet'
    """
    # Mapeamento dos identificadores para os nomes finais desejados
    name_map = {
        'SIMPLES': 'Simples',
        'CNAECSV': 'Cnaes',
        'MOTICSV': 'Motivos',
        'MUNICCSV': 'Municipios',
        'NATJUCSV': 'Naturezas',
        'PAISCSV': 'Paises',
        'QUALSCSV': 'Qualificacoes',
        'EMPRECSV': 'Empresas',
        'ESTABELE': 'Estabelecimentos',
        'SOCIOCSV': 'Socios'
    }

    for key, new_name in name_map.items():
        if key in filename:
            match = re.search(r'Y(\d)', filename)
            number_suffix = match.group(1) if match else ''
            
            return f"{new_name}{number_suffix}.parquet"
            
    logging.warning(f"⚠️ Padrão de nome não reconhecido para '{filename}'.")

    clean_name = filename.split('.')[0].replace('$', '_').replace('.', '_')
    return f"arquivo_{clean_name}.parquet"



def convert_csv_to_parquet():
    try:
        extract_dir = 'backend/etl/extract/'
        parquet_dir = create_parquet_dirs()
        
        csv_files = [
            f for f in os.listdir(extract_dir)
            if 'csv' or 'estabelec' in f.lower() and os.path.isfile(os.path.join(extract_dir, f))
]
        total_files = len(csv_files)
        
        if not csv_files:
            logging.warning("⚠️ Nenhum arquivo CSV encontrado para converter.")
            return
        
        logging.info(f"📁 Encontrados {total_files} arquivo(s) CSV para converter no diretório {extract_dir}.")
        
        conn = duckdb.connect()
        conn.execute("SET threads TO 8;")
        conn.execute("SET memory_limit = '8GB';")
        conn.execute("SET temp_directory = '/tmp/duckdb_temp';")
        
        converted_files = []
        
        for idx, filename in enumerate(csv_files, 1):
            csv_path = os.path.join(extract_dir, filename)
            parquet_name = sanitize_filename(filename)
            parquet_path = os.path.join(parquet_dir, parquet_name)
            
            try:
                logging.info(f"⚙️ [{idx}/{total_files}] Convertendo '{filename}' → '{parquet_name}'")
                
                conn.execute(f"""
                    COPY (
                        SELECT * FROM read_csv_auto(
                            '{csv_path}',
                            delim=';',
                            header=TRUE,
                            ignore_errors=TRUE,
                            encoding='CP1252'
                        )
                    )
                    TO '{parquet_path}' (
                        FORMAT PARQUET,
                        COMPRESSION 'ZSTD'
                    );
                """)
                
                converted_files.append(parquet_path)
                logging.info(f"✅ [{idx}/{total_files}] '{parquet_name}' salvo com sucesso.")
            
            except Exception as e:
                logging.error(f"❌ Erro ao converter '{filename}': {e}")
                continue
        
        conn.close()
        logging.info(f"🎉 Conversão concluída! {len(converted_files)} arquivo(s) convertido(s).")
        return converted_files

    except Exception as e:
        logging.error(f"❌ Erro geral: {e}")
        return None


def convert_parquet_to_ndjson():
    parquet_dir = 'backend/etl/parquet'
    ndjson_dir = 'backend/etl/ndjson'

    os.makedirs(ndjson_dir, exist_ok=True)

    parquet_files = [
        f for f in os.listdir(parquet_dir)
        if f.lower().endswith('.parquet') and os.path.isfile(os.path.join(parquet_dir, f))
    ]

    total_files = len(parquet_files)

    if not parquet_files:
        logging.warning("⚠️ Nenhum arquivo PARQUET encontrado para converter.")
        return []

    logging.info(f"📁 Encontrados {total_files} arquivo(s) PARQUET para converter no diretório {parquet_dir}")

    conn = duckdb.connect()
    conn.execute("SET threads TO 8;")
    conn.execute("SET memory_limit = '8GB';")
    conn.execute("SET temp_directory = '/tmp/duckdb_temp';")

    converted_files = []

    for idx, filename in enumerate(parquet_files, 1):
        parquet_path = os.path.join(parquet_dir, filename)

        # Remove .parquet, limpa e adiciona extensão correta
        base_name = os.path.splitext(filename)[0]
        ndjson_name = f"{base_name}.ndjson"
        ndjson_path = os.path.join(ndjson_dir, ndjson_name)

        try:
            logging.info(f"⚙️ [{idx}/{total_files}] Convertendo '{filename}' → '{ndjson_name}'")

            conn.execute(f"""
                COPY (
                    SELECT * FROM read_parquet('{parquet_path}')
                )
                TO '{ndjson_path}' (
                    FORMAT JSON,
                    ARRAY TRUE
                );
            """)

            converted_files.append(ndjson_name)

        except Exception as e:
            logging.error(f"❌ Erro ao converter '{filename}': {e}")
            continue

    conn.close()
    logging.info(f"🎉 Conversão concluída! {len(converted_files)}/{total_files} arquivo(s) convertidos com sucesso.")
    return converted_files



if __name__ == "__main__":
    convert_csv_to_parquet()
