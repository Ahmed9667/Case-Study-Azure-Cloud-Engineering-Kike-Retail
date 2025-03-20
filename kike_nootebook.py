# Databricks notebook source
#connect notebook with azure blob storage kikeretailstorage

dbutils.fs.mount(
    source = "wasbs://kikeretailcontainer@kikeretailstorage.blob.core.windows.net",  #container@storageaccount
    mount_point = "/mnt/ahmed_blob5", #any name we choose to mount
    extra_configs = {"fs.azure.account.key.kikeretailstorage.blob.core.windows.net":"36BWhvQ8ubKtHKFKeeiF3k14jaVgjQrIdi2C+2n8cCNS4EEST6fR0GRRwYLyEauLJ8WRvRLtv8u6+AStSnoNxQ=="}) #access key of container

# COMMAND ----------

#dsiplay the contents of container
display(dbutils.fs.ls("/mnt/ahmed_blob2"))

# COMMAND ----------

#import the data
import pandas as pd
df = pd.read_csv("/dbfs/mnt/ahmed_blob2/kike_stores_dataset")
df

# COMMAND ----------

#to import spark dataframe
spark.conf.set("fs.azure.account.key.kikeretailstorage.blob.core.windows.net", "36BWhvQ8ubKtHKFKeeiF3k14jaVgjQrIdi2C+2n8cCNS4EEST6fR0GRRwYLyEauLJ8WRvRLtv8u6+AStSnoNxQ==")

# COMMAND ----------

spa = spark.read.csv("/mnt/ahmed_blob2/kike_stores_dataset",header=True,inferSchema=True)
spa.show()

# COMMAND ----------

spa.display()

# COMMAND ----------

spa.count()

# COMMAND ----------

spa = spa.withColumnRenamed('ProductID','Product_ID')
display(spa)

# COMMAND ----------

# MAGIC %md
# MAGIC # Exploratory Data Analysis

# COMMAND ----------

#Drop Duplicated Values
df = df.drop_duplicates()

# COMMAND ----------

df.shape

# COMMAND ----------

# Check for the types of columns and the number of null values
columns = list(df.columns)

type = []
for i in df.columns:
    i_type = str(df[i].dtype)
    type.append(i_type)

nulls=[]
for j in df.columns:
    nulls.append(df[j].isnull().sum())

details = pd.DataFrame()
details['columns'] = columns
details['type'] = type
details['no_of_nulls'] = nulls
details


# COMMAND ----------

# Fill null values
for i in df.columns:
    if df[i].dtype == 'object':
        df[i] = df[i].fillna('unknown')
    else:
         df[i] = df[i].fillna(df[i].mean())   

# COMMAND ----------

#Check for null values after modifying
df.isnull().sum()

# COMMAND ----------

df.columns

# COMMAND ----------

#convert columns of dates to proper formula (datetime)
columns = ['CartDate' , 'OrderDate','ShipmentDate','DeliveryDate','ReturnDate']
for i in columns :
    df[i] = pd.to_datetime(df[i] , errors= 'coerce' )

# COMMAND ----------

df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformation

# COMMAND ----------

df.columns

# COMMAND ----------

#create table products
products_columns = ['ProductID', 'ProductName', 'Category', 'SubCategory', 'Brand','Price', 'Discount', 'Stock','Rating', 'ReviewCount']
products = df[products_columns].drop_duplicates().reset_index(drop=True)

#create table users
users_columns = ['UserID', 'UserName','UserAge', 'UserGender', 'UserLocation']
users = df[users_columns].drop_duplicates().reset_index(drop=True)

#create table carts
carts_columns = ['CartID', 'CartDate','CartTotal', 'CartItemCount', 'CartStatus']
carts = df[carts_columns].drop_duplicates().reset_index(drop=True)

#table orders
orders = df.merge(products, on=products_columns , how='left')\
    .merge(users, on=users_columns,how='left')\
    .merge(carts, on=carts_columns ,how='left')\
    [['OrderID','ProductID','UserID','CartID','OrderDate',\
       'OrderStatus', 'PaymentMethod', 'ShipmentDate', 'DeliveryDate',\
       'ReturnDate', 'RefundAmount', 'ReferralSource', 'PromotionCode']]
print('Created Tables Successfully')   
                    

# COMMAND ----------

orders

# COMMAND ----------

#saving Cleaned Datasets
products.to_csv('/dbfs/mnt/ahmed_blob2/cleaned_data/products.csv',index=False)
users.to_csv('/dbfs/mnt/ahmed_blob2/cleaned_data/users.csv',index=False)
carts.to_csv('/dbfs/mnt/ahmed_blob2/cleaned_data/carts.csv',index=False)
orders.to_csv('/dbfs/mnt/ahmed_blob2/cleaned_data/orderss.csv',index=False)
