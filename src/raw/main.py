import sys
import os
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src/lib')))
import utils
import connection
import mockaroo

import dotenv

dotenv.load_dotenv(".env") #variaveis de ambiente

mockaroo_key = os.getenv("MOCKAROO_API_KEY") #chave de acesso da API


with open('fields.json', 'r') as file:
    fields = json.load(file)

params = {
    "key": mockaroo_key,
    "count": 100
}

def main():

    api_url = "https://api.mockaroo.com/api/generate.json"

    mock = mockaroo.Mockaroo(api_url, fields, params)
    data = mock.generateData()

    if data:
        utils.create_JSON_files(data)
    else:
        return

    path_file = '../../data/raw/'
    file_type = 'json'

    azure_connected = connection.ConnectionBlobStorage('raw', path_file)
    azure_connected.sendFiles(file_type)


if __name__ == "__main__":
    main()




