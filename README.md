<h1 align="center">BuscaCnpjSimples API</h1>

<p align="center">
  <img src="./frontend/assets/favicon.svg" alt="BuscaCnpjSimples Logo" width="120" height="120" />
</p>


<a align="center" href="www.buscacnpjsimples.com.br">Website</a>


## Comece por aqui 
**BuscaCnpjSimples** é uma API pública de alta disponibilidade que usa como base de dados de pessoass jurídicas disponibilizados pela Receita Federal do Brasil. Com base em arquivos `csv`, foi criado um pipeline `ETL` em Python que faz o tratamento dos dados e gera uma API com um único Endpoint de alta disponibilidade e de simples consumo (sem necessida de autenticação).

## Estrutura do Projeto
O projeto tem uma estrutura simples e enxuta que conta as seguintes pastas:
- `backend`: ETL e script para a execução da aplicação que faz download, processa os arquivos e converte para a publicação dos dados. 
- `frontend`: Página simples e estática em HTML que viabiliza a consulta da documentação e chamadas a API. 

## Por que eu criei este projeto? 
- Este projeto iniciou-se como inspiração em diversos projetos que vi no LinkedIn, mas especicamente o projeto [OpenCnpj](https://github.com/Hitmasu/OpenCNPJ). A ideia era fazer algo desafiador e útil para a comunidade e desenvolvedores, enquanto eu melhorava minhas habilidades. 

