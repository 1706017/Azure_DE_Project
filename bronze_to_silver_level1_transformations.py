# Databricks notebook source

 ##1) Doing Transformation for Address Tables



add_df = spark.read.format("parquet")\
    .load("/mnt/bronze/SalesLT/Address/Address.parquet")


display(add_df)



from pyspark.sql.functions import from_utc_timestamp,date_format
from pyspark.sql.types import TimestampType

add_df = add_df.withColumn("ModifiedDate",date_format(from_utc_timestamp(add_df['ModifiedDate'].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))



display(add_df)



display(dbutils.fs.ls("/mnt/bronze/SalesLT/"))




##2) Doing Transformation for Customer Table



cus_df = spark.read.format("parquet").load("/mnt/bronze/SalesLT/Customer/Customer.parquet")

display(cus_df)



mod_cus_df = cus_df.drop("NameStyle","Suffix")
print("Unwanted columns are dropped from Customer data frame")


#updating the datatype from date time to date type only for customer table 

from pyspark.sql.functions import from_utc_timestamp,date_format
from pyspark.sql.types import TimestampType

mod_cus_df = mod_cus_df.withColumn("ModifiedDate",date_format(from_utc_timestamp(mod_cus_df['ModifiedDate'].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))
print("Datetime modified to date only for the column")



#Handling Null for some Columns and replacing with NA

columns_to_replace = ["Title","MiddleName"]
replacement_value = "NA"

replacement_dict = {col: replacement_value for col in columns_to_replace}
mod_cus_df = mod_cus_df.na.fill(replacement_dict)

print("Null values are Handled for titile and MiddleName columns")




 ## 3) Doing Transformation for CustomerAddress table



cus_add_df = spark.read.format("parquet").load("/mnt/bronze/SalesLT/CustomerAddress/CustomerAddress.parquet")
display(cus_add_df)



#updating the datatype from date time to date type only for CustomerAddress Table 

from pyspark.sql.functions import from_utc_timestamp,date_format
from pyspark.sql.types import TimestampType

cus_add_df = cus_add_df.withColumn("ModifiedDate",date_format(from_utc_timestamp(cus_add_df['ModifiedDate'].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))
print("Datetime modified to date only for the column")
display(cus_add_df)


 ##4) Doing Transformation for Product Table 



product_df = spark.read.format("parquet").load("/mnt/bronze/SalesLT/Product/Product.parquet")
display(product_df)




from pyspark.sql.functions import from_utc_timestamp,date_format
from pyspark.sql.types import TimestampType

product_df = product_df.withColumn("ModifiedDate",date_format(from_utc_timestamp(product_df['ModifiedDate'].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))\
                       .withColumn("SellStartDate",date_format(from_utc_timestamp(product_df['SellStartDate'].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))
                
print("Datetime modified to date only for the column")
display(product_df)


#5) Doing Transformation for ProductCategory Table



product_category_df = spark.read.format("parquet").load("/mnt/bronze/SalesLT/ProductCategory/ProductCategory.parquet")



product_category_df = product_category_df.withColumn("ModifiedDate",date_format(from_utc_timestamp(product_category_df['ModifiedDate'].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))
display(product_category_df)


##6) Doing Transformation for ProductDescription Table



product_description_df = spark.read.format("parquet").load("/mnt/bronze/SalesLT/ProductDescription/ProductDescription.parquet")
display(product_description_df)



product_description_df = product_description_df.withColumn("ModifiedDate",date_format(from_utc_timestamp(product_description_df['ModifiedDate'].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))

display(product_description_df)


##7) Doing Transformation for ProductModel Table


product_model_df = spark.read.format("parquet").load("/mnt/bronze/SalesLT/ProductModel/ProductModel.parquet")
display(product_model_df)



product_model_df = product_model_df.withColumn("ModifiedDate",date_format(from_utc_timestamp(product_model_df['ModifiedDate'].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))

display(product_model_df)


##8) Doing Transformation for ProductModelProductDescription Table


product_model_product_des_df = spark.read.format("parquet").load("/mnt/bronze/SalesLT/ProductModelProductDescription/ProductModelProductDescription.parquet")


product_model_product_des_df = product_model_product_des_df.withColumn("ModifiedDate",date_format(from_utc_timestamp(product_model_product_des_df['ModifiedDate'].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))




 ##9) Doing Transformation for SalesOrderDetail Table


sales_order_detail_df = spark.read.format("parquet").load("/mnt/bronze/SalesLT/SalesOrderDetail/SalesOrderDetail.parquet")


sales_order_detail_df = sales_order_detail_df.withColumn("ModifiedDate",date_format(from_utc_timestamp(sales_order_detail_df['ModifiedDate'].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))




##10) Doing Transformation for SalesOrderHeader Table



sales_order_header_df = spark.read.format("parquet").load("/mnt/bronze/SalesLT/SalesOrderHeader/SalesOrderHeader.parquet")


sales_order_header_df = sales_order_header_df.withColumn("ModifiedDate",date_format(from_utc_timestamp(sales_order_header_df['ModifiedDate'].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))\
    .withColumn("OrderDate",date_format(from_utc_timestamp(sales_order_header_df['OrderDate'].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))\
    .withColumn("DueDate",date_format(from_utc_timestamp(sales_order_header_df['DueDate'].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))\
    .withColumn("ShipDate",date_format(from_utc_timestamp(sales_order_header_df['ShipDate'].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))


 ##Now finally Writing the transformed data from Broze layer to Silver layer in Delta format 



sales_order_detail_df.write.format("delta").mode("overwrite").save("/mnt/silver/SalesLT/SalesOrderDetail/")
sales_order_header_df.write.format("delta").mode("overwrite").save("/mnt/silver/SalesLT/SalesOrderHeader/")
product_model_product_des_df.write.format("delta").mode("overwrite").save("/mnt/silver/SalesLT/ProductModelProductDescription/")
product_model_df.write.format("delta").mode("overwrite").save("/mnt/silver/SalesLT/ProductModel/")
product_description_df.write.format("delta").mode("overwrite").save("/mnt/silver/SalesLT/ProductDescription/")
product_category_df.write.format("delta").mode("overwrite").save("/mnt/silver/SalesLT/ProductCategory/")
product_df.write.format("delta").mode("overwrite").save("/mnt/silver/SalesLT/Product/")
cus_add_df.write.format("delta").mode("overwrite").save("/mnt/silver/SalesLT/CustomerAddress/")
mod_cus_df.write.format("delta").mode("overwrite").save("/mnt/silver/SalesLT/Customer/")
add_df.write.format("delta").mode("overwrite").save("/mnt/silver/SalesLT/Address/")




