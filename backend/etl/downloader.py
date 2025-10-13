import asyncio
from bs4 import BeautifulSoup
from datetime import datetime
import os
import logging
import httpx
import aiofiles
from utils import rename_receita_files
import zipfile

logging.basicConfig(level=logging.INFO)


def create_download_and_extraction_dirs():
    download_dir = 'backend/etl/downloads'
    extract_dir = 'backend/etl/extract'

    os.makedirs(download_dir, exist_ok=True)
    os.makedirs(extract_dir, exist_ok=True)

    if os.access(download_dir, os.W_OK) and os.access(extract_dir, os.W_OK):
        return download_dir, extract_dir
    else:
        logging.error(f"Permission denied to the directories {download_dir, extract_dir}.")
        return None, None


def extract_file(zip_path, extract_dir):
    """Extrai um arquivo ZIP de forma síncrona"""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
            logging.info(f"📂 Extraído de/para: {zip_path} -> {extract_dir}")
    except FileNotFoundError:
        logging.error(f"❌ Arquivo não encontrado: {zip_path}")
    except zipfile.BadZipFile:
        logging.error(f"❌ Arquivo mal-formado: {zip_path}")
    except Exception as e:
        logging.error(f"❌ Um erro inesperado ocorreu ao tentar extrair o arquivo: {zip_path}: {e}")


async def download_file_async(file_url, file_path, extract_dir, client):
    max_attempts = 5
    attempts = 0

    while attempts < max_attempts:
        try:
            async with client.stream("GET", file_url) as response:
                response.raise_for_status()

                async with aiofiles.open(file_path, 'wb') as f:
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        if chunk:
                            await f.write(chunk)

            # Verifica se o arquivo foi baixado corretamente
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                logging.info(f"✅ Downloaded: {file_path} ({os.path.getsize(file_path)} bytes)")
                # Extrai o arquivo em uma thread separada
                await asyncio.to_thread(extract_file, file_path, extract_dir)
                return True
            else:
                logging.warning(f"⚠️ Arquivo {file_path} está vazio ou corrompido. Tentando novamente...")

        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            logging.error(f"❌ Erro durante o download {file_url} na tentativa {attempts + 1}: {e}")

        attempts += 1
        if attempts < max_attempts:
            wait_time = 2 ** attempts
            logging.info(f"🔄 Tentando novamente em {wait_time}s... (Tentativa {attempts}/{max_attempts})")
            await asyncio.sleep(wait_time)

    logging.error(f"❌ O download falhou {file_url} depois de {max_attempts} tentativas")
    return False


async def download_all_async():
    download_dir, extract_dir = create_download_and_extraction_dirs()
    if not download_dir:
        return None

    current_date = datetime.now().strftime('%Y-%m')
    base_url = f'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{current_date}/'

    async with httpx.AsyncClient(timeout=httpx.Timeout(600.0, read=600.0)) as client:
        try:
            logging.info(f'Download iniciado em: {datetime.now()}')
            response = await client.get(base_url)
            response.raise_for_status()
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            logging.error(f"URL {base_url} indisponível: {e}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        tasks = []
        for link in soup.find_all('a'):
            href = link.get("href")
            if not href or href == '../' or href.startswith('?') or href.startswith('/'):
                continue

            if '/' not in href and href.endswith('.zip'):
                file_url = f"{base_url}{href}"
                file_path = os.path.join(download_dir, href)


                if os.path.exists(file_path):
                    logging.info(f"⚠️ O arquivo {href} já existe. Extraindo arquivo (se necessário)...")
                    await asyncio.to_thread(extract_file, file_path, extract_dir)
                    continue

                tasks.append(download_file_async(file_url, file_path, extract_dir, client))

        if tasks:
            await asyncio.gather(*tasks)

        for file in os.listdir(download_dir):
            if file.endswith(".zip"):
                file_path = os.path.join(download_dir, file)
                await asyncio.to_thread(extract_file, file_path, extract_dir)
        
        logging.info("🔄 Renomeando arquivos CSV...")
        renamed_files = await asyncio.to_thread(
            rename_receita_files, 
            extract_dir,  
            False  
        )

        logging.info("✅ Downloads e extrações concluído corretamente.")
        return download_dir, extract_dir, renamed_files


if __name__ == "__main__":
    asyncio.run(download_all_async())
