-- Cnpj Data Tables
create table empresas (
	cnpj_basico integer primary key,
	razao_social_nome_empresarial varchar(250) not null,
	natureza_juridica integer not null, 
	qualificao_responsavel integer not null, 
	capital_social_empresa numeric(10,2) not null, 
	porte_empresa integer not null, 
	ente_federativo_responsavel integer not null 
	
)

create table estabelecimentos (
	cnpj_basico integer primary key,
	cnpj_ordem integer,
	cnpj_dv integer,
	identificador_fiscal_matriz_filial integer,
	nome_fantasia varchar(250), 
	situacao_cadastral integer, 
	data_situacao_cadastral date, 
	motivo_situacao_cadastral integer, 
	nome_cidade_exterior varchar(250), 
	pais integer,
	data_inicio_atividade date, 
	cnae_fiscal_principal integer, 
	cnae_fiscal_secundario integer, 
	tipo_logradouro varchar(250), 
	logradouro varchar(250), 
	numero varchar(100), 
	complemento varchar(250), 
	bairro varchar(250), 
	cep integer, 
	uf varchar(2),
	municipio integer, 
	ddd_01 integer, 
	telefone_01 integer, 
	ddd_02 integer, 
	telefone_02 integer, 
	ddd_faz integer, 
	fax integer, 
	correio_eletronico integer, 
	situacao_especial integer,
	data_situacao_especial date 
	
	
)

create table dados_simples (
	cnpj_basico integer primary key, 
	opcao_simples varchar(1), 
	data_opcao_simples date, 
	data_exclusao_simples date,
	opcao_mei varchar(1), 
	data_opcao_mei date, 
	data_exclusao_mei date
	
)


create table socios (
	cnpj_basico integer primary key, 
	identificador_socio integer, 
	nome_socio_razao_social varchar(250), 
	cnpj_cpf_socio integer, 
	qualificao_socio integer, 
	data_entrada_sociedade date, 
	pais integer, 
	representante_legal integer, 
	qualificacao_representante_legal integer,
	faixa_etaria integer

)

-- Information tables
create table paises (
    codigo integer primary key,
    descricao text NOT NULL
)

create table municipios (
	codigo integer primary key, 
	descricao text not null
)

create table qualificacoes_socios (
	codigo integer primary key, 
	descricao varchar(250) not null
)

create table natureza_juridica (
    codigo integer primary key, 
    descricao TEXT not null 
)

create table cnaes (
	codigo integer primary key,
	descricao TEXT not null 
)



