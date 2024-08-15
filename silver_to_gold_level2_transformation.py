# Databricks notebook source
dbutils.fs.ls("/mnt/silver/SalesLT/")

# COMMAND ----------

# MAGIC %md
# MAGIC ##1)Doing Column Name Transformation to Address Data And Writing to Gold Layer
# MAGIC

# COMMAND ----------

address_df = spark.read.format("delta").load("/mnt/silver/SalesLT/Address/")
display(address_df)

address_df = address_df.withColumnRenamed("AddressID","Address_ID")\
          .withColumnRenamed("AddressLine1","Address_Line1")\
          .withColumnRenamed("AddressLine2","Address_Line2")\
          .withColumnRenamed("StateProvince","State_Province")\
          .withColumnRenamed("CountryRegion","Country_Region")\
          .withColumnRenamed("PostalCode","Postal_Code")\
          .withColumnRenamed("rowguid","row_guid")\
          .withColumnRenamed("ModifiedDate","Modified_Date")
display(address_df)

address_df.write.format("delta").mode("overwrite").save("/mnt/gold/SalesLT/Address/")


# COMMAND ----------

# MAGIC %md
# MAGIC ##2)Doing Column Name Transformation to Customer Data And Writing to Gold Layer

# COMMAND ----------

customer_df = spark.read.format("delta").load("/mnt/silver/SalesLT/Customer/")
customer_df = customer_df.withColumnRenamed("CustomerID","Customer_ID")\
           .withColumnRenamed("FirstName","First_Name")\
           .withColumnRenamed("MiddleName","Middle_Name")\
           .withColumnRenamed("LastName","Last_Name")\
           .withColumnRenamed("CompanyName","Company_Name")\
           .withColumnRenamed("SalesPerson","Sales_Person")

customer_df.write.format("delta").mode("overwrite").save("/mnt/gold/SalesLT/Customer/")
           

# COMMAND ----------

# MAGIC %md
# MAGIC ##3) Reading all the data from silver layer and writing to Gold Layer

# COMMAND ----------

customer_address_df = spark.read.format("delta").load("/mnt/silver/SalesLT/CustomerAddress/")
product_df = spark.read.format("delta").load("/mnt/silver/SalesLT/Product/")
product_category_df = spark.read.format("delta").load("/mnt/silver/SalesLT/ProductCategory/")
product_description_df = spark.read.format("delta").load("/mnt/silver/SalesLT/ProductDescription/")
product_model_df = spark.read.format("delta").load("/mnt/silver/SalesLT/ProductModel/")
product_model_product_description_df = spark.read.format("delta").load("/mnt/silver/SalesLT/ProductModelProductDescription/")
sales_order_header_df = spark.read.format("delta").load("/mnt/silver/SalesLT/SalesOrderHeader/")
sales_order_detail_df = spark.read.format("delta").load("/mnt/silver/SalesLT/SalesOrderDetail/")

# COMMAND ----------

customer_address_df.write.format("delta").mode("overwrite").save("/mnt/gold/SalesLT/CustomerAddress/")
product_df.write.format("delta").mode("overwrite").save("/mnt/gold/SalesLT/Product/")
product_category_df.write.format("delta").mode("overwrite").save("/mnt/gold/SalesLT/ProductCategory/")
product_description_df.write.format("delta").mode("overwrite").save("/mnt/gold/SalesLT/ProductDescription/")
product_model_df.write.format("delta").mode("overwrite").save("/mnt/gold/SalesLT/ProductModel/")
product_model_product_description_df.write.format("delta").mode("overwrite").save("/mnt/gold/SalesLT/ProductModelProductDescription/")
sales_order_header_df.write.format("delta").mode("overwrite").save("/mnt/gold/SalesLT/SalesOrderHeader/")
sales_order_detail_df.write.format("delta").mode("overwrite").save("/mnt/gold/SalesLT/SalesOrderDetail/")
