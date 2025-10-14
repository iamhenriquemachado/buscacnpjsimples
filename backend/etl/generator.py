import duckdb
import logging

def query_parquet_with_joins(cnpj_basico=None):

    parquet_dir = 'backend/etl/parquet'
    
    try:
        conn = duckdb.connect()
        conn.execute("SET threads TO 8;")
        conn.execute("SET memory_limit = '8GB';")
        
        logging.info("📊 Registrando arquivos PARQUET como tabelas...")
        
        conn.execute(f"""
            CREATE OR REPLACE VIEW cnaes AS 
            SELECT * FROM read_parquet('{parquet_dir}/cnaes.parquet')
        """)
        
        conn.execute(f"""
            CREATE OR REPLACE VIEW naturezas AS 
            SELECT * FROM read_parquet('{parquet_dir}/naturezas_juridicas.parquet')
        """)
        
        conn.execute(f"""
            CREATE OR REPLACE VIEW qualificacoes AS 
            SELECT * FROM read_parquet('{parquet_dir}/qualificacoes.parquet')
        """)
        
        conn.execute(f"""
            CREATE OR REPLACE VIEW municipios AS 
            SELECT * FROM read_parquet('{parquet_dir}/municipios.parquet')
        """)
        
        conn.execute(f"""
            CREATE OR REPLACE VIEW empresas AS 
            SELECT * FROM read_parquet('{parquet_dir}/empresas*.parquet')
        """)
        
        conn.execute(f"""
            CREATE OR REPLACE VIEW estabelecimentos AS 
            SELECT * FROM read_parquet('{parquet_dir}/estabelecimentos*.parquet')
        """)
        
        conn.execute(f"""
            CREATE OR REPLACE VIEW socios AS 
            SELECT * FROM read_parquet('{parquet_dir}/socios*.parquet')
        """)

        conn.execute(f"""
                CREATE OR REPLACE VIEW simples AS 
                SELECT * FROM read_parquet('{parquet_dir}/simples.parquet')
            """)
        
        logging.info("✅ Tabelas registradas com sucesso!")
        
        query = build_join_query(33671831)
        
        logging.info("🔍 Executando query com JOINs...")
        result = conn.execute(query).fetchall()
        
        conn.close()
        logging.info(f"✅ Query executada! {len(result)} registros retornados.")
        
        return result
        
    except Exception as e:
        logging.error(f"❌ Erro ao executar query: {e}")
        return None


def build_join_query(cnpj_basico=None):
    
    where_clause = f"WHERE e.cnpj_basico = '{cnpj_basico}'" if cnpj_basico else ""
    
    query = f"""
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
        e.municipio,
        mun.descricao as municipio,
        e.uf,
        sim.opcao_mei,
        sim.data_opcao_mei
    FROM estabelecimentos e
    LEFT JOIN empresas emp ON e.cnpj_basico = emp.cnpj_basico
    LEFT JOIN simples sim ON e.cnpj_basico = sim.cnpj_basico
    LEFT JOIN naturezas nat ON emp.natureza_juridica = nat.codigo
    LEFT JOIN municipios mun ON e.municipio = mun.codigo
    {where_clause}
    """
    
    return query


if __name__ == "__main__":
    query_parquet_with_joins()