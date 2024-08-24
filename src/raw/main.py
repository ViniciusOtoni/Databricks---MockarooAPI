import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src/lib')))
import utils
import connection

import requests
import dotenv


dotenv.load_dotenv(".env") #variaveis de ambiente
newsapi_key = os.getenv("NEWSAPI_KEY") #chave de acesso da API


params = {
    'q': 'economy', #target
    'language': 'en',
    'pageSize': 100, 
    'apiKey': newsapi_key
}

response = requests.get("https://newsapi.org/v2/everything", params=params)

if response.status_code == 200:
    data = response.json()
    utils.create_JSON_files(data)
else:
    print(f"erro na requisição: {response.status_code}")



path_file = '../../data/raw/'
file_type = 'json'

azure_connected = connection.ConnectionBlobStorage('raw', path_file)
azure_connected.sendFiles(file_type)


