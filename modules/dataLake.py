from azure.storage.filedatalake import DataLakeServiceClient
from modules.KeyVault import get_sas_token, get_storage_account_name


def initialize_storage_account_sas(storage_account_name, sas_token: str):
    try:  
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format("https", storage_account_name), credential=sas_token)
        return service_client
    except Exception as e:
        print(e)

def create_directory(service_client: DataLakeServiceClient, file_system: str, directory: str):
    try:
        file_system_client = service_client.get_file_system_client(file_system=file_system)
        file_system_client.create_directory(directory)
        directory_client = file_system_client.get_directory_client(directory=directory)
        return directory_client
    except Exception as e:
     print(e)

def get_directory(service_client: DataLakeServiceClient, file_system: str, directory: str):
    try:       
       file_system_client = service_client.get_file_system_client(file_system=file_system)
       directory_client = file_system_client.get_directory_client(directory=directory)       
       return directory_client
    except Exception as e:
     print(e)

def upload_file_to_directory_bulk(directory_client, file_name_client, local_file_path):
    try:
        file_client = directory_client.get_file_client(file_name_client)
        local_file = open(local_file_path,'r')
        file_contents = local_file.read()
        file_client.upload_data(file_contents, overwrite=True)
    except Exception as e:
      print(e)
