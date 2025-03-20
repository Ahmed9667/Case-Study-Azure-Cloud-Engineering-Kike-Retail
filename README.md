# Kike Retail Data Pipeline

## Executive Summary:
Kike Retailis a leading e-commerce platform specializing in a wide range of
products, from electronics to fashion. With a rapidly growing customer base
and an expanding product catalog, Kike Retail faces challenges in efficiently
managing and analyzing vast amounts of data generated daily. To streamline
operations and enhance decision-making, Kike Retail aims to leverage Azure
cloud data engineering solutions to transform their data processing and
analytics capabilities.


## Objectives:
 `●` Implement a robust data pipeline to automate data extraction,
transformation, and loading (ETL) processes.

`●` Enhance data quality and consistency through effective data cleaning and
transformation techniques.

`●` Enable scalable and efficient data storage and management.

`●` Provide timely and actionable insights to support business decisions.

`●` Ensure data security and compliance with industry standards.


## Tech Stack:
For this case study, the following tech stack was employed:
A. Python: For scripting and automation of data processes.
B. SQL: For querying and managing relational databases.
C. Azure Blob Storage: For scalable data storage.
D. Azure Data Factory: For orchestrating data workflows and ETL processes.
E. Azure Databricks: For big data processing and advanced analytics.
F. Store API: For extracting raw data from Kike Retail' systems.


## Data Architecture:

![image](https://github.com/user-attachments/assets/b4dcdc68-3946-4ae1-a148-bc284f91e308)


## Business Problem Statement:
Kike Retail struggles with managing large volumes of data from multiple
sources, leading to inefficiencies in data processing and analysis. The
current system lacks automation, resulting in time-consuming and
error-prone manual processes. This limits the ability to gain timely insights
and make data-driven decisions to improve business performance and
customer satisfaction.
The code reads a CSV file from a URL into a pandas DataFrame, prepares the file for upload by converting it into an in-memory CSV format,
and uploads the data in chunks to Azure Blob Storage. Environment variables store sensitive information such as the Azure connection string and container name.

### Steps of Uploading In Case Of Large Dataset:

#### 1.Chunk size and file setup:
-The chunk_size is set to 4MB (4 * 1024 * 1024 bytes), meaning each chunk of data that is uploaded will be of this size.

-files is a list of tuples. Here, it contains the DataFrame (df) and the blob name 'kike_stores_dataset'.
```python
chunk_size = 4 * 1024 * 1024
files = [(df, 'kike_stores_dataset')]
```

#### 2.Creating an in-memory file object:
-A StringIO object is created, which is a file-like object that resides in memory.

-The DataFrame (file) is converted to CSV format using the to_csv method, and written into the output buffer in memory.

-seek(0) ensures that the file pointer is at the beginning of the in-memory CSV data, so it can be read from the start when uploading.

```python
output = io.StringIO()
file.to_csv(output, index=False)
output.seek(0)
```


#### 3.Defining a generator function to split the file into chunks:
-The generate_chunks function reads the output file in chunks of chunk_size (4MB in this case).

-It yields each chunk until the entire file is read.
```python
def generate_chunks(file_stream, chunk_size):
    while True:
        chunk = file_stream.read(chunk_size)
        if not chunk:
            break
        yield chunk
```


#### 4.Uploading the file in chunks:
-A BlobClient is created for the specified blob_name, which represents the file to be uploaded.

-The upload_blob method uploads the file in chunks by passing the generator generate_chunks.

-The overwrite=True flag ensures that if the file already exists, it will be overwritten.
```python
blob_client.upload_blob(generate_chunks(output, chunk_size), overwrite=True)
```


#### 5.Exception handling:
If there’s an error during the upload (e.g., network issues or incorrect permissions), an exception is caught, and an error message is printed.
```python
except Exception as e:
    print(f"Failed to upload {blob_name}: {str(e)}")
```

----
## Run Dag with Apache Airflow:

![image](https://github.com/user-attachments/assets/a2e5a3f7-151b-4927-ad2c-87c0fb3aa73a)

and then make sure dataset is uploaded successfully

![image](https://github.com/user-attachments/assets/eafacb00-d44d-429a-a8ee-38758e13697d)

## Create Cluster on Azure Databricks to operate the uloaded data

![image](https://github.com/user-attachments/assets/fd78376c-47ae-4d3b-a77e-353a9902e5bd)








