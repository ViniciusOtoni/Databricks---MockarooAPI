# Databricks notebook source
import sys
import os

sys.path.insert(0, '../../src/lib')
import utils
from ingestors import IngestaoBronze


# COMMAND ----------

catalog = dbutils.widgets.get("catalog") #"hive_metastore" 
database = dbutils.widgets.get("database") #"bronze"
tableName = dbutils.widgets.get("tablename") #"news_economy"

# COMMAND ----------

account_name = "storagenewsapi" #storage
account_key = os.getenv("BLOB_STORAGE_ACCOUNT_KEY") #(variavel de ambiente colocada no cluster.)

# COMMAND ----------

container_name = "raw"
source_url = f"wasbs://{container_name}@{account_name}.blob.core.windows.net"

conf_key = f"fs.azure.account.key.{account_name}.blob.core.windows.net"
mount_name = f"/mnt/project/raw/{database}/full_load/{tableName}" 


# COMMAND ----------

utils.create_mount(spark, mount_name, source_url, conf_key, account_key)

# COMMAND ----------

if not utils.table_exists(spark, catalog, database, tableName):
    print("Tabela não existe, criando...")

    ingest_full_load = IngestaoBronze(
                                    spark = spark,
                                    catalog_name=catalog,
                                    table_name=tableName, 
                                    database_name=database,
                                    file_format='json',
                                    partition_fields="source.name"
                                )
    
    ingest_full_load.execute(f"/mnt/project/raw/{database}/full_load/{tableName}")

    print("Tabela criada com sucesso!")
else:
    print("Tabela já existe, ignorando full-load")

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
