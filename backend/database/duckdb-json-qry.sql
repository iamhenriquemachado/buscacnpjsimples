SELECT 
    e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv as cnpj, 
    COALESCE(emp.razao_social, ''), 
    COALESCE(emp.natureza_juridica, ''), 
    COALESCE(e.nome_fantasia, ''),
    situacao_cadastra := CASE WHEN e.situacao_cadastral = 01 THEN '01 - Nula'
                              WHEN e.situacao_cadastral = 2 THEN '2 - Ativa'
                              WHEN e.situacao_cadastral = 3 THEN '3 - Suspensa'
                              WHEN e.situacao_cadastral = 4 THEN '4 - Inapta'
                              WHEN e.situacao_cadastral = 08 THEN '08 - Baixada'
    e.data_situacao_cadastral, 
    e.motivo_motivo_situacao_cadastral,
    e.data_inicio_atividade,
    c.codigo || c.descricao, 
    cnaes_secundarios := array_agg(
        COALESCE(cna.cnae_fiscal_secundaria, '') || COALESCE(cna.descricao, '')
    ),
    natureza_juridica := nat.codigo || nat.descricao, 
    qualificacao_responsavel := q.codigo || q.descricao, 
    capital_social := emp.capital_social, 
    porte_empresa := CASE WHEN emp.porte_empresa = 00 THEN '00 - Não Informado'
                          WHEN emp.porte_empresa = 01 THEN '01 - Micro Empresa'
                          WHEN emp.porte_empresa = 03 THEN '03 - Empresa de Pequeno Porte'
                          WHEN emp.porte_empresa = 05 THEN '05 - Demais'
                    END, 
    ente_federativo_responsavel := emp.ente_federativo_responsavel,

    tipo_logradouro := e.tipo_logradouro
    logradouro := e.logradouro, 
    numero := e.numero, 
    complemento := e.complemento, 
    bairro := e.bairro, 
    cep := e.cep,
    uf := e.uf, 
    municipio := e.municipio,
    telefones := array_agg(struct_pack(
        e.ddd1 || e.telefone_1, 
        e.ddd2 || e.telefone_2
    ))




    FROM estabelecimentos e
    LEFT JOIN empresas emp ON e.cnpj_basico = emp.cnpj_basico
    LEFT JOIN simples sim ON e.cnpj_basico = sim.cnpj_basico
    LEFT JOIN naturezas nat ON emp.natureza_juridica = nat.codigo
    LEFT JOIN municipios mun ON e.municipio = mun.codigo
    LEFT JOIN cnaes c on e.cnae_fiscal_principal = c.codigo
    LEFT JOIN cnaes cna on e.cnae_fiscal_secundaria = c.codigo
    LEFT JOIN qualificacoes q on emp.qualificacao_responsavel = q.codigo

  