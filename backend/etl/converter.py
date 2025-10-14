import os
import logging
import ftfy
import unicodedata
import duckdb
import re
from pathlib import Path
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')


COLUMN_MAPPINGS = {
    'estabelec': [
        'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial',
        'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral',
        'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais',
        'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria',
        'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro',
        'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2',
        'ddd_fax', 'fax', 'correio_eletronico', 'situacao_especial',
        'data_situacao_especial'
    ],
    'empres': [
        'cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel',
        'capital_social', 'porte_empresa', 'ente_federativo_responsavel'
    ],
    'socio': [
        'cnpj_basico', 'identificador_socio', 'nome_socio', 'cnpj_cpf_socio',
        'qualificacao_socio', 'data_entrada_sociedade', 'pais', 'representante_legal',
        'nome_representante', 'qualificacao_representante_legal', 'faixa_etaria'
    ],
    'simples': [
        'cnpj_basico', 'opcao_simples', 'data_opcao_simples', 'data_exclusao_simples',
        'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei'
    ],
    'cnae': [
        'codigo', 'descricao'
    ],
    'moti': [
        'codigo', 'descricao'
    ],
    'munic': [
        'codigo', 'descricao'
    ],
    'natju': [
        'codigo', 'descricao'
    ],
    'pais': [
        'codigo', 'descricao'
    ],
    'quals': [
        'codigo', 'descricao'
    ]
}

def get_column_names(filename):
    """Retorna os nomes das colunas baseado no tipo de arquivo"""
    filename_lower = filename.lower()
    
    # Mapeamento por palavras-chave nos nomes dos arquivos
    if 'simples' in filename_lower:
        return COLUMN_MAPPINGS['simples']
    elif 'empre' in filename_lower or 'empresa' in filename_lower:
        return COLUMN_MAPPINGS['empres']
    elif 'socio' in filename_lower:
        return COLUMN_MAPPINGS['socio']
    elif 'estabele' in filename_lower:
        return COLUMN_MAPPINGS['estabelec']
    elif 'cnae' in filename_lower:
        return COLUMN_MAPPINGS['cnae']
    elif 'moti' in filename_lower:
        return COLUMN_MAPPINGS['moti']
    elif 'munic' in filename_lower:
        return COLUMN_MAPPINGS['munic']
    elif 'natju' in filename_lower or 'natureza' in filename_lower:
        return COLUMN_MAPPINGS['natju']
    elif 'pais' in filename_lower:
        return COLUMN_MAPPINGS['pais']
    elif 'qual' in filename_lower:
        return COLUMN_MAPPINGS['quals']
    
    logging.warning(f"⚠️ Nenhum mapeamento encontrado para '{filename}'. Usando auto-detecção.")
    return None

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
    """Converte nome do arquivo CSV para Parquet"""
    # Remove extensões e códigos, mantém apenas o tipo
    name_lower = filename.lower()
    
    if 'simples' in name_lower:
        base_name = 'simples'
    elif 'empre' in name_lower:
        base_name = 'empresas'
    elif 'socio' in name_lower:
        base_name = 'socios'
    elif 'estabele' in name_lower:
        base_name = 'estabelecimentos'
    elif 'cnae' in name_lower:
        base_name = 'cnaes'
    elif 'moti' in name_lower:
        base_name = 'motivos'
    elif 'munic' in name_lower:
        base_name = 'municipios'
    elif 'natju' in name_lower or 'natureza' in name_lower:
        base_name = 'naturezas_juridicas'
    elif 'pais' in name_lower:
        base_name = 'paises'
    elif 'qual' in name_lower:
        base_name = 'qualificacoes'
    else:
        base_name = filename.split('.')[0]
    
    match = re.search(r'Y(\d+)', filename.upper())
    if match:
        sequence = match.group(1)
        base_name += sequence
    
    return f"{base_name}.parquet"



def convert_csv_to_parquet():
    try:
        extract_dir = 'backend/etl/extract/'
        parquet_dir = create_parquet_dirs()
        
        csv_files = [
            f for f in os.listdir(extract_dir)
            if os.path.isfile(os.path.join(extract_dir, f))
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
                
                # Obter nomes das colunas
                column_names = get_column_names(filename)
                
                if column_names:
                    # Criar query com nomes de colunas específicos
                    columns_str = ', '.join([f'"{col}"' for col in column_names])
                    
                    conn.execute(f"""
                        COPY (
                            SELECT * FROM read_csv(
                                '{csv_path}',
                                delim=';',
                                header=FALSE,
                                columns={{{', '.join([f"'{col}': 'VARCHAR'" for col in column_names])}}},
                                ignore_errors=TRUE,
                                encoding='CP1252',
                                dateformat='%Y%m%d'
                            )
                        )
                        TO '{parquet_path}' (
                            FORMAT PARQUET,
                            COMPRESSION 'ZSTD'
                        );
                    """)
                    logging.info(f"✅ [{idx}/{total_files}] '{parquet_name}' salvo com {len(column_names)} colunas nomeadas.")
                else:
                    # Fallback: usar auto-detecção
                    conn.execute(f"""
                        COPY (
                            SELECT * FROM read_csv_auto(
                                '{csv_path}',
                                delim=';',
                                header=FALSE,
                                ignore_errors=TRUE,
                                encoding='CP1252'
                            )
                        )
                        TO '{parquet_path}' (
                            FORMAT PARQUET,
                            COMPRESSION 'ZSTD'
                        );
                    """)
                    logging.info(f"✅ [{idx}/{total_files}] '{parquet_name}' salvo (auto-detecção).")
                
                converted_files.append(parquet_path)
            
            except Exception as e:
                logging.error(f"❌ Erro ao converter '{filename}': {e}")
                continue
        
        conn.close()
        logging.info(f"🎉 Conversão concluída! {len(converted_files)} arquivo(s) convertido(s).")
        return converted_files

    except Exception as e:
        logging.error(f"❌ Erro geral: {e}")
        return None
    
if __name__ == "__main__":
    convert_csv_to_parquet()
