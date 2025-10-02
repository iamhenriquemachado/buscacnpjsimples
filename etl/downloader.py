import asyncio
from bs4 import BeautifulSoup
from datetime import datetime
import os
import logging
import httpx
import aiofiles

logging.basicConfig(level=logging.INFO)


def createCnpjDataDirectory():
    path = 'cnpj_data'
    os.makedirs(path, exist_ok=True)
    check_access = os.access(path, os.W_OK)
    if check_access:
        return path
    else:
        logging.error("Permission denied. Please grant write access to the folder.")
        return None


async def download_file_async(file_url, file_path, client):
    try:
        async with client.stream("GET", file_url) as response:
            response.raise_for_status()
            async with aiofiles.open(file_path, 'wb') as f:
                async for chunk in response.aiter_bytes(chunk_size=8192):
                    if chunk:
                        await f.write(chunk)
        logging.info(f"✅ Downloaded: {file_path} ({os.path.getsize(file_path)} bytes)")
    except httpx.RequestError as e:
        logging.error(f"Error downloading {file_url}: {e}")


async def download_all_async():
    cnpj_data_directory = createCnpjDataDirectory()
    if not cnpj_data_directory:
        return None

    current_date = '2025-09'  # datetime.now().strftime('%Y-%m')
    base_url = f'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{current_date}/'

    async with httpx.AsyncClient(timeout=None) as client:
        # Verifica se a URL principal está disponível
        try:
            response = await client.get(base_url)
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
                file_path = os.path.join(cnpj_data_directory, href)
                tasks.append(download_file_async(file_url, file_path, client))

        if tasks:
            # Executa downloads em paralelo
            await asyncio.gather(*tasks)

    logging.info("All files downloaded successfully!")
    return cnpj_data_directory


if __name__ == "__main__":
    asyncio.run(download_all_async())
