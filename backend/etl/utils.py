import logging 
from pathlib import Path

logging.basicConfig(level=logging.INFO)

def normalize_csv_filenames(original_name):
        
        name = original_name.upper()

        mappings = {
        'SIMPLES': 'simples',
        'SOCIO': 'socios',
        'EMPRESA': 'empresas',
        'ESTABELE': 'estabelecimentos',
        'CNAE': 'cnaes',
        'MOTIVO': 'motivos',
        'NATUREZA': 'naturezas_juridicas',
        'QUALIFICACAO': 'qualificacoes',
        'PAIS': 'paises',
        'MUNICIPIO': 'municipios',
    }
        
        for pattern, normalized in mappings.item():
                if pattern in name:
                    return f"{normalized}.csv"

def rename_receita_files(extract_dir='backend/etl/extract/', dry_run=False):
    """
    Renomeia arquivos da Receita Federal para padrão consistente
    
    Args:
        extract_dir: Diretório com os arquivos extraídos
        dry_run: Se True, apenas simula sem renomear
    
    Returns:
        dict: Mapeamento {nome_antigo: nome_novo}
    """
    
    try:
        extract_path = Path(extract_dir)
        
        if not extract_path.exists():
            logging.error(f"❌ Diretório não encontrado: {extract_dir}")
            return None
        
        renamed_files = {}
        duplicates = {}
        
        # Lista todos os arquivos
        files = [f for f in os.listdir(extract_dir) if os.path.isfile(os.path.join(extract_dir, f))]
        
        if not files:
            logging.warning("⚠️ Nenhum arquivo encontrado no diretório.")
            return {}
        
        logging.info(f"📁 Encontrados {len(files)} arquivo(s) para processar")
        
        # Primeira passagem: gera novos nomes e detecta duplicatas
        for filename in files:
            new_name = normalize_csv_filenames(filename)
            
            # Detecta duplicatas
            if new_name in duplicates.values():
                # Adiciona sufixo numérico para evitar colisão
                base, ext = new_name.rsplit('.', 1)
                counter = 1
                while f"{base}_{counter}.{ext}" in duplicates.values():
                    counter += 1
                new_name = f"{base}_{counter}.{ext}"
            
            duplicates[filename] = new_name
            renamed_files[filename] = new_name
        
        # Segunda passagem: renomeia os arquivos
        for old_name, new_name in renamed_files.items():
            old_path = extract_path / old_name
            new_path = extract_path / new_name
            
            if old_name == new_name:
                logging.info(f"⏭️  '{old_name}' → já está padronizado")
                continue
            
            if dry_run:
                logging.info(f"🔍 [DRY RUN] '{old_name}' → '{new_name}'")
            else:
                try:
                    old_path.rename(new_path)
                    logging.info(f"✅ '{old_name}' → '{new_name}'")
                except Exception as e:
                    logging.error(f"❌ Erro ao renomear '{old_name}': {e}")
        
        if dry_run:
            logging.info("🔍 Modo DRY RUN - nenhum arquivo foi renomeado")
        else:
            logging.info(f"🎉 {len(renamed_files)} arquivo(s) processado(s)!")
        
        return renamed_files
        
    except Exception as e:
        logging.error(f"❌ Erro ao renomear arquivos: {e}")
        return None