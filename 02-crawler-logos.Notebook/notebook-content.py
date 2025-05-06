# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9ffbd5ea-e9c7-4221-ae18-5aeb48c2725e",
# META       "default_lakehouse_name": "lake_ibba",
# META       "default_lakehouse_workspace_id": "cd40bef7-7875-42e3-92f6-ffd71d32a223",
# META       "known_lakehouses": [
# META         {
# META           "id": "9ffbd5ea-e9c7-4221-ae18-5aeb48c2725e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
from bs4 import BeautifulSoup
import os

# URL da página
url = "https://companiesmarketcap.com/brazil/largest-companies-in-brazil-by-market-cap/"

# Criar pasta para salvar as imagens
#pasta = "logos_empresas"
#os.makedirs(pasta, exist_ok=True)

# Caminho da pasta no Lakehouse (área de arquivos)
pasta_lakehouse = "/lakehouse/default/Files/logos_empresas/"
os.makedirs(pasta_lakehouse, exist_ok=True)  # Em notebooks do Fabric, isso garante que a pasta seja criada

# Obter o conteúdo da página
response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")

# Encontrar todas as imagens
imagens = soup.find_all("img")
url_home = "https://companiesmarketcap.com"
# Baixar e salvar as imagens
for i, img in enumerate(imagens):
    src = img.get("src")
    alt = img.get("alt").replace(" logo","") 
    #alt = alt.replace(" logo","")    
    if "/img/company-logos" in src:
       # print(src,alt)
        img_data = requests.get(url_home+src).content
        file_name = src.split("/")[-1]

        #with open(f"{pasta}/{file_name}", "wb") as file:
        #    file.write(img_data)

        with open(f"{pasta_lakehouse}/{file_name}", "wb") as file:
            file.write(img_data)

print("Download concluído!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
