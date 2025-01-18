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
#and make data-driven decisions to improve business performance and
customer satisfaction.
The code reads a CSV file from a URL into a pandas DataFrame, prepares the file for upload by converting it into an in-memory CSV format,
and uploads the data in chunks to Azure Blob Storage. Environment variables store sensitive information such as the Azure connection string and container name.

### Steps of Uploading Large Dataset:

#### Chunk size and file setup:
-The chunk_size is set to 4MB (4 * 1024 * 1024 bytes), meaning each chunk of data that is uploaded will be of this size.
-files is a list of tuples. Here, it contains the DataFrame (df) and the blob name 'kike_stores_dataset'.
```python
chunk_size = 4 * 1024 * 1024
files = [(df, 'kike_stores_dataset')]
```







