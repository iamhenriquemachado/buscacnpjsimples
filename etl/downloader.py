import asyncio
from bs4 import BeautifulSoup
from datetime import datetime
import os
import logging
import httpx
import aiofiles
import zipfile


logging.basicConfig(level=logging.INFO)


def createDownloadAndExtractionDirectory():
    download_dir = 'etl/downloads'
    extract_dir = 'etl/extract'

    os.makedirs(download_dir, exist_ok=True)
    os.makedirs(extract_dir, exist_ok=True)

    check_acces_download_dir = os.access(download_dir, os.W_OK)
    check_acces_extract_dir= os.access(extract_dir, os.W_OK)

    if check_acces_download_dir and check_acces_extract_dir:
        return download_dir
    else:
        logging.error(f"Permission denied to the directories {download_dir, extract_dir}.")
        return None


async def download_file_async(file_url, file_download_dir, client):
    max_attempts = 5
    attempts = 0

    while attempts < max_attempts:
        try:
            async with client.stream("GET", file_url) as response:
                response.raise_for_status()  # Raise error if >= 400

                async with aiofiles.open(file_download_dir, 'wb') as f:
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        if chunk:
                            await f.write(chunk)

            # Check if the file is downloaded
            if os.path.exists(file_download_dir) and os.path.getsize(file_download_dir) > 0:
                logging.info(f"✅ Downloaded: {file_download_dir} ({os.path.getsize(file_download_dir)} bytes)")
                return True  
            else:
                logging.warning(f"⚠️ File {file_download_dir} is empty or corrupted. Retrying...")

        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            logging.error(f"❌ Error downloading {file_url} on attempt {attempts + 1}: {e}")

        # Retry logic
        attempts += 1
        if attempts < max_attempts:
            wait_time = 2 ** attempts 
            logging.info(f"🔄 Retrying in {wait_time}s... (Attempt {attempts}/{max_attempts})")
            await asyncio.sleep(wait_time)

    logging.error(f"❌ Failed to download {file_url} after {max_attempts} attempts")
    return False



def extractFilesAsync(download_path, extract_path):
    try:
        with zipfile.ZipFile(download_path, 'r') as zip:
            zip.extractall(extract_path)
            logging.info(f'The file {download_path} was extracted to {extract_path} directory.')

    except FileNotFoundError as e:
        logging.error(f'The file was not found: {e}')
    except Exception as e:
        logging(f'An exception ocurred: {e}')

async def download_all_async():
    cnpj_data_directory = createDownloadAndExtractionDirectory()
    if not cnpj_data_directory:
        return None

    current_date = '2025-09'  # datetime.now().strftime('%Y-%m')
    base_url = f'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{current_date}/'

    async with httpx.AsyncClient(timeout=10.0) as client:
        # Verifica se a URL principal está disponível
        try:
            logging.info(f'Downloading process started at: {datetime.now()}')

            response = await client.get(base_url, timeout=10)
            response.raise_for_status()
        except httpx.RequestError as e:
            logging.error(f"The URL {base_url} is unavailable: {e}")
            return None

        logging.info(f"Accessing: {base_url}")
        html = response.text
        soup = BeautifulSoup(html, "html.parser")

        # Construir lista de tarefas para downloads
        tasks = []
        for link in soup.find_all('a'):
            href = link.get("href")

            if not href or href == '../' or href.startswith('?') or href.startswith('/'):
                continue

            if '/' not in href and href.endswith('.zip'):
                file_url = f"{base_url}{href}"
                file_download_dir = os.path.join(cnpj_data_directory, href)

                if os.path.exists(file_download_dir):
                    logging.info(f"⚠️ The file {href} already exists in the download_dir {file_download_dir}. The next file will be verified and downloaded.")
                    continue

                tasks.append(download_file_async(file_url, file_download_dir, client))

        if tasks:
            # Executa downloads em paralelo
            await asyncio.gather(*tasks)
        

        logging.info("All files downloaded successfully!")
        extractFilesAsync(file_download_dir, extract_path='extract')

        return cnpj_data_directory


# if __name__ == "__main__":
#     asyncio.run(download_all_async())

extractFilesAsync('etl/downloads', 'extract')