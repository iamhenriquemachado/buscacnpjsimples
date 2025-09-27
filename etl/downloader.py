import requests
from bs4 import BeautifulSoup

from utils.utils import createCnpjDataDirectory, getCurrentMonthAndYear

get_current_date = getCurrentMonthAndYear()

url = f'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{get_current_date}/Cnaes.zip'
response = requests.get(url)

soup = BeautifulSoup(response.text, "html.parser")

links = soup.find_all("a")

# for link in links:
#     print(link.get("href"))

with open("Cnaes.zip", "w", encoding="utf-8") as file:
        file.write(response.text)

print("Arquivo Cnaes.zip salvo!")