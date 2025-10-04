## Converter .csv para .parquet

 - [x] Listar todos os arquivos .csv em etl/extract
 - [x] Criar pasta etl/parquet (se não existir)
 - [] Para cada arquivo .csv:
  - [ ] Ler o arquivo e converter para .parquet
  - [ ] Salvar em etl/parquet com o mesmo nome
  - [ ] Validar se o .parquet foi criado com sucesso
  - [ ] Se OK → apagar .csv de extract e zip de downloads
  - [ ] Se erro → mover .csv para etl/error
 - [] Registrar logs de sucesso e erro