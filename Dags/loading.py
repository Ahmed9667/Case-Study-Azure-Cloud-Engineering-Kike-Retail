import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError
from dotenv import load_dotenv
import os
import io

# Loading data into Azure Blob Storage
def run_loading():
    # Read the CSV file from the URL
    df = pd.read_csv(r'https://raw.githubusercontent.com/Ahmed9667/Case-Study---Azure-Cloud-Engineering-Kike-Retail/refs/heads/main/kike_stores_dataset.csv')

    # Load environment variables
    load_dotenv()

    # Retrieve the connection string and container name from environment variables
    connect_str = os.getenv('AZURE_CONNECTION_STRING_VALUE')
    container_name = os.getenv('CONTAINER_NAME')

    # Create the BlobServiceClient and ContainerClient objects
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    chunk_size = 4 * 1024 * 1024
    files = [(df,'kike_stores_dataset')]

    for file, blob_name in files:
        blob_client = container_client.get_blob_client(blob_name)

        # Convert DataFrame to CSV in memory
        output = io.StringIO()  # Create an in-memory string buffer
        file.to_csv(output, index=False)
        output.seek(0)  # Go to the beginning of the file-like object

        # Define a generator function to yield chunks of the file
        def generate_chunks(file_stream, chunk_size):
            while True:
                chunk = file_stream.read(chunk_size)
                if not chunk:
                    break
                yield chunk

        try:
            # Upload the file in chunks
            blob_client.upload_blob(generate_chunks(output, chunk_size), overwrite=True)
            print(f'{blob_name} loaded into Azure Blob Storage successfully')

        except Exception as e:
            print(f"Failed to upload {blob_name}: {str(e)}")


    