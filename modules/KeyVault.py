from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient


credential = AzureCliCredential()
secret_client = SecretClient(vault_url="https://kv-kpd-scus-epl-dev.vault.azure.net/", credential=credential)

def get_sas_token():
    sas_token_secret = secret_client.get_secret("AzureDataLakeStorage-sakpdscusepl-landing-SAStoken-upload")
    return sas_token_secret.value

def get_storage_account_name():
    storage_account_name_secret = secret_client.get_secret("AzureDataLakeStorage-sakpdscusepl-StorageAccountName")
    return storage_account_name_secret.value