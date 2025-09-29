import requests
from bs4 import BeautifulSoup
from datetime import datetime 

url = f'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/'

current_date = datetime.now().strftime('%d-%m-%Y')
print(current_date)