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

!pip install cairosvg

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import requests
import cairosvg
from bs4 import BeautifulSoup
from IPython.display import display, SVG

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

caminho_csv = "Files/data/tickers_b3.csv"

df = spark.read.option("header", True).option("inferSchema", True).option("delimiter", ";").csv(caminho_csv)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#tickers = df.select("ticker").limit(10).collect()
tickers = df.select("ticker").collect()

for item in tickers:
    ticker = item['ticker']

    # URL da página
    url = f"https://br.tradingview.com/symbols/BMFBOVESPA-{ticker}/"
    
    # Criar pasta para salvar as imagens
    # Caminho da pasta no Lakehouse (área de arquivos)
    pasta_svg = "/lakehouse/default/Files/logos_svg/"
    pasta_png = "/lakehouse/default/Files/logos_png/"

    # Em notebooks do Fabric, isso garante que a pasta seja criada
    os.makedirs(pasta_svg, exist_ok=True)  
    os.makedirs(pasta_png, exist_ok=True)

    # Obter o conteúdo da página
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    
    # Encontrar todas as imagens
    imagens = soup.find_all("img")
    url_home = "https://br.tradingview.com"

    # Baixar e salvar as imagens
    for i, img in enumerate(imagens):
        src = img.get("src")
        
        if i>0 and "https://s3-symbol-logo.tradingview.com/" in src:
            img_data = requests.get(src).content
            file_name_svg = f"{ticker}.svg"
            file_name_png = f"{ticker}.png"
            
            with open(f"{pasta_svg}/{file_name_svg}", "wb") as file:
                file.write(img_data)

            cairosvg.svg2png(url=f"{pasta_svg}/{file_name_svg}", write_to=f"{pasta_png}/{file_name_png}", dpi=300, scale=2)

            print(file_name_png)

            #display(SVG(f"{pasta_svg}/{file_name}"))

            break
    
print("Download concluído!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
