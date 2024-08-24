import pandas as pd
import datetime
import json
import os


def create_JSON_files(data):

    output_dir = '../../data/raw'
    os.makedirs(output_dir, exist_ok=True)

    articles = data['articles']

    df = pd.DataFrame(articles)

    for index, row in df.iterrows(): #itera cada linha do df.
        
        row_dict = row.to_dict() #criando o dicionário para o json transformando cada linha em dicionário

        published_date = datetime.datetime.strptime(row_dict['publishedAt'], '%Y-%m-%dT%H:%M:%SZ')

        file_name = f"{published_date.strftime('%d-%m-%Y_%H-%M-%S')}.json" #gerando nome do arquivo JSON
        file_path = os.path.join(output_dir, file_name) #enviando os dados para o diretório

        with open(file_path, 'w') as json_file:
            json.dump(row_dict, json_file) #salvar o dicionario como arquivo JSON

    print("arquivos criados com sucesso!")