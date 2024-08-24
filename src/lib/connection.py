from azure.storage.blob import BlobServiceClient
import dotenv
import os

dotenv.load_dotenv(".env")

class ConnectionBlobStorage:

    def __init__(self, container_name, path_file):
        self.account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
        self.account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
        self.container_name = container_name
        self.local_json_directory = path_file
        self.container_client = self.connectionStorage()

    def connectionStorage(self):

        connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.account_name};AccountKey={self.account_key};EndpointSuffix=core.windows.net"
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(self.container_name)

        return container_client


    def sendFiles(self, file_type):
        for root, dirs, files in os.walk(self.local_json_directory):
            for file_name in files:
                if file_name.endswith(f'.{file_type}'):  # pegando o tipo do arquivo
                    file_path = os.path.join(root, file_name)
                    
                    # verificando caso o arquivo exista.
                    if not os.path.isfile(file_path):
                        print(f"arquivo não existe: {file_path}")
                        continue

                    # criando blob para o arquivo
                    blob_name = file_name
                    blob_client = self.container_client.get_blob_client(blob_name)
      
                    try:
                        with open(file_path, "rb") as data:
                            blob_client.upload_blob(data, overwrite=True) #enviando blob para o container
                            print(f"arquivo {blob_name} enviado com sucesso para o container {self.container_name}!")

                        #remove os arquivos depois do upload na cloud.
                        os.remove(file_path)
                        print(f"arquivo {file_name} removido localmente com sucesso!")

                    except Exception as e:
                        print(f"erro ao enviar o arquivo {blob_name}: {str(e)}")

