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
import time

from pyspark.sql.functions import *
from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Carrega lista de Tickers a serem processados
schema = StructType() \
 .add("ticker", StringType(), True) \
 .add("NomeEmpresa", StringType(), True) \
 .add("RazaoSocial", StringType(), True) 

caminho_csv = "Files/data/tickers_b3.csv"
df_tickers = spark.read.option("header", True).schema(schema).option("delimiter", ";").csv(caminho_csv)
df_tickers.printSchema()

#Filtra
#df_tickers = df_tickers.filter(lower(col("NomeEmpresa")).contains("ambev"))

tickers = df_tickers.select("ticker").collect()
len(tickers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_fundamentus_data(ticker):
    url = f"http://fundamentus.com.br/detalhes.php?papel={ticker}"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    
    data = {
        'Ticker': ticker,
        'Empresa': None,
        'Setor': None,
        'SubSetor': None,
        'MarketCapital': None,
        'EV_EBITDA': None,
        'P_L': None,
        'MargemEBIT': None
    }
    
    try:
        tables = soup.find_all("table")
        for table in tables:
            for row in table.find_all("tr"):
                cols = [td.text.strip() for td in row.find_all("td")]
                if "?Empresa" in cols:
                    data['Empresa'] = cols[cols.index("?Empresa")+1]
                if "?Setor" in cols:
                    data['Setor'] = cols[cols.index("?Setor")+1]
                    data["Setor"] = data.get("Setor") or "Outros"
                if "?Subsetor" in cols:
                    data['SubSetor'] = cols[cols.index("?Subsetor")+1]
                    data["SubSetor"] = data.get("SubSetor") or "Outros"
                if "?Valor de mercado" in cols:
                    value = cols[cols.index("?Valor de mercado")+1]
                    value = "0,00" if value in ["-", "", "0"] else value
                    data['MarketCapital'] = float(value.replace('.', '').replace(',', '.')[:-1]) / 1000000
                if "?EV / EBITDA" in cols:
                    value = cols[cols.index("?EV / EBITDA")+1]
                    value = "0,00" if value in ["-", "", "0"] else value
                    data['EV_EBITDA'] = float(value.replace('.', '').replace(',', '.'))
                if "?P/L" in cols:
                    value = cols[cols.index("?P/L")+1]
                    value = "0,00" if value in ["-", "", "0"] else value
                    data['P_L'] = float(value.replace('.', '').replace(',', '.'))
                if "?Marg. EBIT" in cols:
                    value = cols[cols.index("?Marg. EBIT")+1]
                    value = "0,00" if value in ["-", "", "0"] else value
                    value = value.replace('.', '').replace(',', '.').replace('%', '').strip()
                    data['MargemEBIT'] = float(value) if value else 0.0

                    #data['MargemEBIT'] = float(value.replace('%', '').replace(',', '.').replace('-', '0').replace('', '0'))

    except Exception as e:
        print(f"Erro ao processar {ticker}: {e}")
        print(data)
    return data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

# Coleta de dados para todos os tickers
dados = []
i=0
k=0

for item in tickers:
    ticker = item['ticker']
    k = k+1
    if i >= 30:
        i=0
        print(f'Tickers processados: {((k/1045)*100):.1f}%')
        #print(f'Tickers processados: {k}')
    else:
        i=i+1

    dados.append(get_fundamentus_data(ticker))
    time.sleep(1)  # respeitar o site


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Converter em DataFrame

# define the schema
schema = StructType() \
 .add("Ticker", StringType(), True) \
 .add("Empresa", StringType(), True) \
 .add("Setor", StringType(), True) \
 .add("SubSetor", StringType(), True) \
 .add("MarketCapital", DoubleType(), True) \
 .add("EV_EBITDA", DoubleType(), True) \
 .add("P_L", DoubleType(), True) \
 .add("MargemEBIT", DoubleType(), True)


df = spark.createDataFrame(dados)
df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DROP da tabela se já existir
spark.sql("DROP TABLE IF EXISTS indicadores_fundamentus")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Salva o df em tabela no DataLake
df.select(['Ticker','Empresa','Setor','SubSetor','MarketCapital','EV_EBITDA','P_L','MargemEBIT']) \
    .write.format("delta") \
    .mode('overwrite') \
    .saveAsTable("indicadores_fundamentus")

display(df.limit(5))

print('Job executado com sucesso! :)')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
