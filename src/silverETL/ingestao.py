# Databricks notebook source
import sys
sys.path.insert(0, "../lib")

import utils
import ingestors
import pipeline

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
table = dbutils.widgets.get("tablename")
database = dbutils.widgets.get("database")

table_full_name = f'{catalog}.{database}.{table}'

# COMMAND ----------

query = utils.import_query(f"{table}.sql")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS silver;

# COMMAND ----------

df = spark.sql(query)

if table == 'movies':

    pipe = pipeline.Pipe(df, 'movie_genre')

    df = pipe.execute()

# COMMAND ----------

if not utils.table_exists(spark, catalog, database, table):
    print("Tabela não existe, criando...")

    ingest_full_load = ingestors.IngestaoSilver(
                                    spark = spark,
                                    catalog_name=catalog,
                                    table_name=table, 
                                    database_name=database,
                                    file_format='delta',
                                )
    
    ingest_full_load.execute(df)

    print("Tabela criada com sucesso!")
else:
    print("Tabela já existe, ignorando full-load")
