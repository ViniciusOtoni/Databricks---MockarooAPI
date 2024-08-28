# Databricks notebook source
import sys
sys.path.insert(0, "../lib")
import utils
import ingestors

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS hive_metastore.gold;

# COMMAND ----------

catalog = "hive_metastore"
database = "gold" 
table = dbutils.widgets.get("tablename") 
conditional = dbutils.widgets.get("conditionalSql") 
conditionalValue = dbutils.widgets.get("conditionalSqlValue") 

# COMMAND ----------

ingest = ingestors.IngestorCubo(spark=spark,
                                    catalog=catalog,
                                    databasename=database,
                                    tablename=table)
    
ingest.execute(conditional=f'{conditionalValue}')
