### 🧩 **Arquivo 1 – Conversão CSV → Parquet**

#### 📁 Diretórios e estrutura

* `test_create_parquet_dirs_creates_if_not_exists` → cria diretório corretamente se não existir
* `test_create_parquet_dirs_returns_existing` → retorna caminho existente se o diretório já existir
* `test_create_parquet_dirs_handles_error` → retorna `None` e loga erro em caso de exceção

#### 🧹 Limpeza e normalização de texto

* `test_clean_text_fixes_encoding` → corrige texto com caracteres estranhos
* `test_clean_text_returns_non_string_as_is` → mantém tipos não string inalterados

#### 🔄 Conversão CSV → Parquet

* `test_convert_csv_to_parquet_converts_valid_csv` → converte CSV válido em arquivo Parquet
* `test_convert_csv_to_parquet_applies_clean_text` → aplica função `clean_text` em todas as células
* `test_convert_csv_to_parquet_handles_no_csv_files` → retorna `None` se não houver CSVs no diretório
* `test_convert_csv_to_parquet_handles_missing_directory` → trata erro de diretório inexistente
* `test_convert_csv_to_parquet_handles_corrupted_csv` → ignora ou loga erro em CSV corrompido

---

### 🌐 **Arquivo 2 – Download e extração**

#### 📁 Criação de diretórios

* `test_create_download_and_extraction_dirs_creates_successfully` → cria diretórios de download e extração
* `test_create_download_and_extraction_dirs_handles_permission_error` → retorna `None` se sem permissão

#### 🗜️ Extração de arquivos ZIP

* `test_extract_file_valid_zip` → extrai corretamente arquivos válidos
* `test_extract_file_missing_zip` → loga erro se o arquivo ZIP não existir
* `test_extract_file_corrupted_zip` → loga erro para ZIP inválido

#### 🌍 Download individual

* `test_download_file_async_success` → baixa arquivo com sucesso e extrai
* `test_download_file_async_retry_on_failure` → tenta novamente após falha de rede
* `test_download_file_async_max_attempts_reached` → retorna `False` após exceder tentativas
* `test_download_file_async_empty_file` → loga aviso se o arquivo baixado estiver vazio

#### 🔗 Download e parsing geral

* `test_download_all_async_creates_dirs` → cria diretórios no início do processo
* `test_download_all_async_handles_unreachable_url` → retorna `None` se a URL da Receita estiver offline
* `test_download_all_async_parses_links_correctly` → identifica corretamente links válidos de ZIP
* `test_download_all_async_skips_invalid_links` → ignora links inválidos ou diretórios
* `test_download_all_async_extracts_existing_zips` → reprocessa ZIPs já baixados
* `test_download_all_async_final_log_success` → confirma mensagem final de sucesso

---

### ⚙️ **Extras recomendados**

* `test_pipeline_integration_csv_to_parquet_after_download` → garante que arquivos baixados são transformados corretamente
* `test_performance_csv_to_parquet_large_file` → mede tempo de conversão em arquivo grande
* `test_end_to_end_download_extract_convert` → executa todo o fluxo simulado (mockando rede e arquivos)
