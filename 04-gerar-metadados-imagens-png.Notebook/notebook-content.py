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

from pyspark.sql import SparkSession
import os
import datetime

# Definir caminho da pasta
folder_path = "/Files/logos_png"
folder_path = "/lakehouse/default/Files/logos_png/"

# Listar arquivos na pasta
image_files = [f for f in os.listdir(folder_path) if f.endswith(".png")]

# Criar lista de metadados
metadata_list = []
for file_name in image_files:
    file_path = os.path.join(folder_path, file_name)
    last_modified = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
    file_size = os.path.getsize(file_path)
    file_type = "PNG"

    metadata_list.append((file_name, file_path, last_modified, file_size, file_type))

# Criar DataFrame Spark
columns = ["file_name", "file_path", "last_modified", "file_size", "file_type"]
df = spark.createDataFrame(metadata_list, columns)

# Salvar no Lakehouse
df.write.mode("overwrite").write.format("delta").saveAsTable("MetadataImagens")

print("Metadados inseridos na tabela com sucesso!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
