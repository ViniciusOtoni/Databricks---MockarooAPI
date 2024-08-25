import pandas as pd
import datetime
import json
import os

def create_mount(spark, mount_name, source_url, conf_key, account_key):
    
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    
    # Verifica se o ponto de montagem já existe
    if any(mount.mountPoint == mount_name for mount in dbutils.fs.mounts()):
        print(f"O ponto de montagem '{mount_name}' já existe.")
    else:
        # Montar o Blob Storage
        dbutils.fs.mount(
            source=source_url,
            mount_point=mount_name,
            extra_configs={conf_key: account_key}
        )

def table_exists(spark, catalog, database, table):
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                .filter(f"database = '{database}' AND tableName = '{table}'")
                .count())
    return count == 1


def create_JSON_files(data):

    output_dir = '../../data/raw'
    os.makedirs(output_dir, exist_ok=True)

    
    df = pd.DataFrame(data)

    for index, row in df.iterrows(): #itera cada linha do df.
        
        row_dict = row.to_dict() #criando o dicionário para o json transformando cada linha em dicionário

        release_date = datetime.datetime.strptime(row_dict['release_date'], '%Y-%m-%dT%H:%M:%S%z')
        file_name = f"{release_date.strftime('%d-%m-%Y_%H-%M-%S')}.json" # Gerando nome do arquivo JSON

        file_path = os.path.join(output_dir, file_name) #enviando os dados para o diretório

        with open(file_path, 'w') as json_file:
            json.dump(row_dict, json_file) #salvar o dicionario como arquivo JSON

    print("arquivos criados com sucesso!")