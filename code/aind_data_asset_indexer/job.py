from aind_data_access_api.document_store import DocumentStoreCredentials, Client as DsClient
from aind_codeocean_api.codeocean import CodeOceanClient

def run_job(aws_secrets_name: str, collection_name:str):
    doc_store_creds = DocumentStoreCredentials(
        aws_secrets_name=aws_secrets_name
    )

    doc_store_client = DsClient(
        credentials=doc_store_creds,
        collection_name=collection_name
    )

    current_records = doc_store_client.retrieve_data_asset_records()
