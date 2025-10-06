import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os 
import logging
import ftfy
import unicodedata
import chardet

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


def convert_csv_to_parquet():
    try:
        extract_dir = 'backend/etl/extract/'
        parquet_dir = 'backend/etl/parquet/'
        
        converted_files = []
        
        for filename in os.listdir(extract_dir):
            if filename.endswith('.csv'):
                file_path = os.path.join(extract_dir, filename)
                
                logging.info(f"📄 Lendo o arquivo CSV: '{file_path}'...")
                
                df = pd.read_csv(
                    file_path,  
                    sep=';',
                    encoding='cp1252', 
                    header=None,
                    quotechar='"',
                    on_bad_lines='warn',
                    dtype=str 
                )
                
                logging.info("⚙️  Normalizando os dados de texto...")
                df = df.applymap(clean_text)
                
                logging.info("🔄 Convertendo para o formato Parquet...")
                table = pa.Table.from_pandas(df)
                
                parquet_filename = filename.replace('.csv', '.parquet')
                parquet_file_path = os.path.join(parquet_dir, parquet_filename)
                
                pq.write_table(table, parquet_file_path, compression='snappy')
                
                logging.info(f"✅ Conversão concluída! Arquivo salvo em: '{parquet_file_path}'")
                print(df.head(10))
                
                converted_files.append(parquet_file_path)
        
        if converted_files:
            logging.info(f"🎉 Total de {len(converted_files)} arquivo(s) convertido(s)!")
            return converted_files
        else:
            logging.warning("⚠️  Nenhum arquivo CSV encontrado para converter.")
            return None
            
    except FileNotFoundError as e:
        logging.error(f"❌ Erro: Diretório não encontrado - {e}")
        return None
    except Exception as e:
        logging.error(f"❌ Ocorreu um erro inesperado durante a conversão: {e}")
        return None



# Executar programa 
if __name__ == "__main__":
     convert_csv_to_parquet()