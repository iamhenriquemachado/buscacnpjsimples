import requests
from bs4 import BeautifulSoup
from datetime import datetime 
import os


def createCnpjDataDirectory():
    os.makedirs('etl/cnpj_data', exist_ok=True)
    return 'cnpj_data'

def download_archive(url=None):
    try:
        cnpj_data_directory = createCnpjDataDirectory()

        current_date = datetime.now().strftime('%Y-%m')
        base_url = f'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{current_date}/'
        
        print(f"Accessing: {base_url}")
        response = requests.get(base_url)
        response.raise_for_status()

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            for link in soup.find_all('a'): 
                href = link.get("href")

                # Skip empty, parent directory, and query links
                if not href or href == '../' or href.startswith('?') or href.startswith('/'):
                    continue

                # Build the complete URL
                if '/' not in href:
                    file_url = f"{base_url}{href}"
                else:
                    # Skip if it's a full path
                    continue
                
                filename = href
                file_path = os.path.join(cnpj_data_directory, filename)

                print(f"Downloading: {filename}")
                print(f"From URL: {file_url}")

                file_response = requests.get(file_url, stream=True)
                file_response.raise_for_status()

                with open(file_path, 'wb') as file:
                    for chunk in file_response.iter_content(chunk_size=8192):
                        if chunk:
                            file.write(chunk)
                
                print(f"✓ Downloaded: {filename} ({os.path.getsize(file_path)} bytes)\n")
            
            print("All files downloaded successfully!")
            return cnpj_data_directory
        
    except requests.exceptions.RequestException as e:
        print(f"Error while downloading file: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None


download_archive()