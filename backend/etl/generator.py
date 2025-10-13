import duckdb
import logging

def query_ndjson_with_joins(cnpj_prefix=None):
    """
    Executa queries com JOINs entre múltiplos arquivos NDJSON usando DuckDB.
    
    Args:
        cnpj_prefix: Prefixo do CNPJ para filtrar (opcional)
    """
    ndjson_dir = 'backend/etl/ndjson'
    
    try:
        conn = duckdb.connect()
        conn.execute("SET threads TO 8;")
        conn.execute("SET memory_limit = '8GB';")
        
        # Registrar todos os arquivos NDJSON como tabelas
        logging.info("📊 Registrando arquivos NDJSON como tabelas...")
        
        # Tabelas auxiliares/lookup
        conn.execute(f"""
            CREATE OR REPLACE VIEW cnaes AS 
            SELECT * FROM read_json_auto('{ndjson_dir}/Cnaes.ndjson', format='array')
        """)
        
        conn.execute(f"""
            CREATE OR REPLACE VIEW naturezas AS 
            SELECT * FROM read_json_auto('{ndjson_dir}/Naturezas.ndjson', format='array')
        """)
        
        conn.execute(f"""
            CREATE OR REPLACE VIEW qualificacoes AS 
            SELECT * FROM read_json_auto('{ndjson_dir}/Qualificacoes.ndjson', format='array')
        """)
        
        conn.execute(f"""
            CREATE OR REPLACE VIEW municipios AS 
            SELECT * FROM read_json_auto('{ndjson_dir}/Municipios.ndjson', format='array')
        """)
        
        # Empresas (pode ter múltiplos arquivos)
        conn.execute(f"""
            CREATE OR REPLACE VIEW empresas AS 
            SELECT * FROM read_json_auto('{ndjson_dir}/Empresas*.ndjson', format='array')
        """)
        
        # Estabelecimentos
        conn.execute(f"""
            CREATE OR REPLACE VIEW estabelecimentos AS 
            SELECT * FROM read_json_auto('{ndjson_dir}/Estabelecimentos*.ndjson', format='array')
        """)
        
        # Sócios
        conn.execute(f"""
            CREATE OR REPLACE VIEW socios AS 
            SELECT * FROM read_json_auto('{ndjson_dir}/Socios*.ndjson', format='array')
        """)
        
        # Simples (se existir)
        try:
            conn.execute(f"""
                CREATE OR REPLACE VIEW simples AS 
                SELECT * FROM read_json_auto('{ndjson_dir}/Simples*.ndjson', format='array')
            """)
        except:
            logging.warning("⚠️ Arquivo Simples não encontrado, continuando sem ele...")
        
        logging.info("✅ Tabelas registradas com sucesso!")
        
        # Agora você pode fazer queries com JOINs
        query = build_join_query(cnpj_prefix)
        
        logging.info("🔍 Executando query com JOINs...")
        result = conn.execute(query).fetchall()
        
        # Ou retornar como DataFrame
        # df = conn.execute(query).df()
        
        conn.close()
        logging.info(f"✅ Query executada! {len(result)} registros retornados.")
        
        return result
        
    except Exception as e:
        logging.error(f"❌ Erro ao executar query: {e}")
        return None


def build_join_query(cnpj_prefix=None):
    """
    Constrói a query SQL com JOINs entre as tabelas.
    Similar ao exemplo em C# que você forneceu.
    """
    
    where_clause = f"WHERE e.cnpj_prefix = '{cnpj_prefix}'" if cnpj_prefix else ""
    
    query = f"""
    WITH socios_data AS (
        SELECT 
            s.cnpj_basico,
            array_agg(struct_pack(
                nome_socio := COALESCE(s.nome_socio, ''),
                cnpj_cpf_socio := COALESCE(s.cnpj_cpf_socio, ''),
                qualificacao_socio := COALESCE(q.descricao, ''),
                data_entrada_sociedade := CASE 
                    WHEN s.data_entrada_sociedade ~ '^[0-9]{{8}}$' 
                    THEN SUBSTRING(s.data_entrada_sociedade, 1, 4) || '-' || 
                         SUBSTRING(s.data_entrada_sociedade, 5, 2) || '-' || 
                         SUBSTRING(s.data_entrada_sociedade, 7, 2)
                    ELSE COALESCE(s.data_entrada_sociedade, '')
                END,
                identificador_socio := CASE s.identificador_socio
                    WHEN '1' THEN 'Pessoa Jurídica'
                    WHEN '2' THEN 'Pessoa Física'
                    WHEN '3' THEN 'Estrangeiro'
                    ELSE COALESCE(s.identificador_socio, '')
                END,
                faixa_etaria := CASE s.faixa_etaria
                    WHEN '0' THEN 'Não se aplica'
                    WHEN '1' THEN '0 a 12 anos'
                    WHEN '2' THEN '13 a 20 anos'
                    WHEN '3' THEN '21 a 30 anos'
                    WHEN '4' THEN '31 a 40 anos'
                    WHEN '5' THEN '41 a 50 anos'
                    WHEN '6' THEN '51 a 60 anos'
                    WHEN '7' THEN '61 a 70 anos'
                    WHEN '8' THEN '71 a 80 anos'
                    WHEN '9' THEN 'Mais de 80 anos'
                    ELSE COALESCE(s.faixa_etaria, '')
                END
            )) as qsa_data
        FROM socios s
        LEFT JOIN qualificacoes q ON s.qualificacao_socio = q.codigo
        {where_clause.replace('e.', 's.')}
        GROUP BY s.cnpj_basico
    )
    SELECT 
        e.cnpj_basico,
        e.cnpj_ordem,
        e.cnpj_dv,
        emp.razao_social,
        emp.natureza_juridica,
        nat.descricao as natureza_juridica_descricao,
        e.nome_fantasia,
        e.situacao_cadastral,
        e.data_situacao_cadastral,
        e.codigo_municipio,
        mun.descricao as municipio,
        e.uf,
        sd.qsa_data as socios,
        sim.opcao_pelo_simples,
        sim.data_opcao_simples
    FROM estabelecimentos e
    LEFT JOIN empresas emp ON e.cnpj_basico = emp.cnpj_basico
    LEFT JOIN simples sim ON e.cnpj_basico = sim.cnpj_basico
    LEFT JOIN naturezas nat ON emp.natureza_juridica = nat.codigo
    LEFT JOIN municipios mun ON e.codigo_municipio = mun.codigo
    LEFT JOIN socios_data sd ON e.cnpj_basico = sd.cnpj_basico
    WHERE e.cnpj_basico = ''''
    LIMIT 100
    """
    
    return query


if __name__ == "__main__":
    build_join_query()