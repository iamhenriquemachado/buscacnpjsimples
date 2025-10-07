import logging 
from pathlib import Path
import os
import re

logging.basicConfig(level=logging.INFO)


def sanitize_filename(filename):
    # Remove extensão e caracteres indesejados
    name = filename.replace('CSV', '').replace('.csv', '').replace('.CSV', '')
    name = re.sub(r'[^A-Za-z0-9_.-]', '_', name)
    name = re.sub(r'_+', '_', name)  # remove underscores duplos
    name = name.strip('_').upper()
    
    # Extrai um identificador útil (ex: SIMPLES, PAIS, SOCIO)
    match = re.search(r'(SIMPLES|CNAE|MOTIV|NATJU|QUAL|EMPRE|ESTABELE|SOCIOS)', name)
    identifier = match.group(1) if match else "ARQUIVO"
    
    # Gera nome final
    return f"{identifier}_{name}.parquet"


for f in os.listdir('backend/etl/downloads'):
    print(f)

