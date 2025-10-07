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
