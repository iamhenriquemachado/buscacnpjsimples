import asyncio
from bs4 import BeautifulSoup
from datetime import datetime
import os
import logging
import httpx
import aiofiles

logging.basicConfig(level=logging.INFO)


def createDownloadAndExtractionDirectories():
    download_dir = 'downloads'
    extract_dir = 'extract'

    os.makedirs(download_dir, exist_ok=True)
    os.makedirs(extract_dir, exist_ok=True)

    check_acces_download_dir = os.access(download_dir, os.W_OK)
    check_acces_extract_dir= os.access(extract_dir, os.W_OK)

    if check_acces_download_dir and check_acces_extract_dir:
        return download_dir
    else:
        logging.error("Permission denied. Please grant write access to the folder.")
        return None


async def download_file_async(file_url, file_download_dir, client):
    try:
        async with client.stream("GET", file_url) as response:
            response.raise_for_status()
            async with aiofiles.open(file_download_dir, 'wb') as f:
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    if chunk:
                        await f.write(chunk)
        logging.info(f"✅ Downloaded: {file_download_dir} ({os.download_dir.getsize(file_download_dir)} bytes)")
    except httpx.RequestError as e:
        logging.error(f"Error downloading {file_url}: {e}")


async def download_all_async():
    cnpj_data_directory = createDownloadAndExtractionDirectories()
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
                file_download_dir = os.download_dir.join(cnpj_data_directory, href)

                if os.download_dir.exists(file_download_dir):
                    logging.info(f"⚠️ The file {href} already exists in the download_dir {file_download_dir}. The next file will be verified and downloaded.")
                    continue

                tasks.append(download_file_async(file_url, file_download_dir, client))

        if tasks:
            # Executa downloads em paralelo
            await asyncio.gather(*tasks)
        

        logging.info("All files downloaded successfully!")
        return cnpj_data_directory


if __name__ == "__main__":
    asyncio.run(download_all_async())
