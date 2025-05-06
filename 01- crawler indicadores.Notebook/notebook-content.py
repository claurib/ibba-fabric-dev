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
from datetime import datetime
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
        'ticker': ticker,
        'empresa': None,
        'data_balanco': None,
        'setor': None,
        'sub_setor': None,
        'market_capital': None,
        'ev_ebitda': None,
        'p_l': None,
        'margem_ebit': None
    }
    
    try:
        tables = soup.find_all("table")
        for table in tables:
            for row in table.find_all("tr"):
                cols = [td.text.strip() for td in row.find_all("td")]
                if "?Empresa" in cols:
                    data['empresa'] = cols[cols.index("?Empresa")+1]
                if "?Últ balanço processado" in cols:
                    value = cols[cols.index("?Últ balanço processado")+1]
                    data['data_balanco'] = datetime.strptime(value, "%d/%m/%Y").strftime("%Y-%m-%d")
                if "?Setor" in cols:
                    data['setor'] = cols[cols.index("?Setor")+1]
                    data["setor"] = data.get("setor") or "Outros"
                if "?Subsetor" in cols:
                    data['sub_setor'] = cols[cols.index("?Subsetor")+1]
                    data["sub_setor"] = data.get("sub_setor") or "Outros"
                if "?Valor de mercado" in cols:
                    value = cols[cols.index("?Valor de mercado")+1]
                    value = "0,00" if value in ["-", "", "0"] else value
                    data['market_capital'] = float(value.replace('.', '').replace(',', '.')[:-1]) / 1000000
                if "?EV / EBITDA" in cols:
                    value = cols[cols.index("?EV / EBITDA")+1]
                    value = "0,00" if value in ["-", "", "0"] else value
                    data['ev_ebitda'] = float(value.replace('.', '').replace(',', '.'))
                if "?P/L" in cols:
                    value = cols[cols.index("?P/L")+1]
                    value = "0,00" if value in ["-", "", "0"] else value
                    data['p_l'] = float(value.replace('.', '').replace(',', '.'))
                if "?Marg. EBIT" in cols:
                    value = cols[cols.index("?Marg. EBIT")+1]
                    value = "0,00" if value in ["-", "", "0"] else value
                    value = value.replace('.', '').replace(',', '.').replace('%', '').strip()
                    data['margem_ebit'] = float(value) if value else 0.0

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
    if i >= 100:
        i=0
        print(f'Tickers processados: {((k/len(tickers))*100):.1f}%')
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
 .add("ticker", StringType(), True) \
 .add("empresa", StringType(), True) \
 .add("data_balanco", StringType(), True) \
 .add("setor", StringType(), True) \
 .add("sub_detor", StringType(), True) \
 .add("market_capital", DoubleType(), True) \
 .add("ev_ebitda", DoubleType(), True) \
 .add("p_l", DoubleType(), True) \
 .add("margem_ebit", DoubleType(), True)

ano_mes = datetime.now().strftime("%Y-%m")

df = spark.createDataFrame(dados)
df = df.withColumn('origem_dados', lit('Fundamentus')).withColumn('ano_mes', lit(ano_mes))
display(df.limit(5))
df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DROP da tabela se já existir
spark.sql("DROP TABLE IF EXISTS indicadores_b3")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Salvar como Parquet
df.coalesce(1).write.mode("overwrite").parquet("Files/data/indicadores_b3.parquet")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Salva o df em tabela no DataLake
df.select([
    'ano_mes',
    'ticker',
    'origem_dados',
    'empresa',
    'data_balanco',
    'setor',
    'sub_setor',
    'market_capital',
    'ev_ebitda','p_l','margem_ebit']) \
    .write.format("delta") \
    .mode('overwrite') \
    .saveAsTable("indicadores_b3")

print('Job executado com sucesso! :)')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
