SELECT 
	e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv as cnpj, 
	emp.razao_social
FROM read_parquet('C:/Users/heyhe/development/buscacnpjsimples/backend/etl/parquet/estabelecimentos0.parquet') AS e
LEFT JOIN read_parquet('C:/Users/heyhe/development/buscacnpjsimples/backend/etl/parquet/empresas1.parquet') AS emp
    ON e.cnpj_basico = emp.cnpj_basico
LEFT JOIN read_parquet('C:/Users/heyhe/development/buscacnpjsimples/backend/etl/parquet/simples.parquet') AS sim
    ON e.cnpj_basico = sim.cnpj_basico
LEFT JOIN read_parquet('C:/Users/heyhe/development/buscacnpjsimples/backend/etl/parquet/naturezas_juridicas.parquet') AS nat
    ON emp.natureza_juridica = nat.codigo
LEFT JOIN read_parquet('C:/Users/heyhe/development/buscacnpjsimples/backend/etl/parquet/municipios.parquet') AS mun
    ON e.municipio = mun.codigo
LEFT JOIN read_parquet('C:/Users/heyhe/development/buscacnpjsimples/backend/etl/parquet/cnaes.parquet') AS c
    ON e.cnae_fiscal_principal = c.codigo
WHERE e.cnpj_basico IN ('15164610',
'15164625',
'15164640',
'15164657',
'15164666',
'15164681',
'15164696',
'15164712',
'15164728',
'15164743',
'15164758',
'15164778',
'7206816',
'15164799',
'15164804',
'15164822',
'15164842',
'15164857',
'15164871',
'20428973',
'15109478',
'15164898',
'15164918',
'15164930',
'15164949',
'15164960',
'15164996',
'15165015',
'15165032',
'15165045',
'15165057',
'15165070',
'15165086',
'15165102',
'15165121',
'15165132',
'15165147',
'15165167',
'15165183',
'15165197',
'15165211',
'15165222',
'15165238',
'15165255',
'15165273',
'15165292',
'15165307',
'15165321',
'15165337',
'15165355')
