# Databricks notebook source
# MAGIC %md
# MAGIC ### Creating Catalog & Schema

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *




# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS my_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS my_catalog.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS my_catalog.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading From Bronze Layer

# COMMAND ----------

df_cust_info = spark.read.format('parquet')\
    .option('header', True)\
    .option('inferSchema', True)\
    .load(f"abfss://bronze@datalakeAbhaykr07.dfs.core.windows.net/source_crm/cust_info.parquet")

# COMMAND ----------

df_prd_info = spark.read.format('parquet')\
    .option('header', True)\
    .option('inferSchema', True)\
    .load(f"abfss://bronze@datalakeAbhaykr07.dfs.core.windows.net/source_crm/prd_info.parquet")

# COMMAND ----------

df_sales_details = spark.read.format('parquet')\
    .option('header', True)\
    .option('inferSchema', True)\
    .load(f"abfss://bronze@datalakeAbhaykr07.dfs.core.windows.net/source_crm/sales_details.parquet")

# COMMAND ----------

df_cust_az12 = spark.read.format('parquet')\
    .option('header', True)\
    .option('inferSchema', True)\
    .load(f"abfss://bronze@datalakeAbhaykr07.dfs.core.windows.net/source_erp/CUST_AZ12.parquet")

# COMMAND ----------

df_loc_a101 = spark.read.format('parquet')\
    .option('header', True)\
    .option('inferSchema', True)\
    .load(f"abfss://bronze@datalakeAbhaykr07.dfs.core.windows.net/source_erp/LOC_A101.parquet")

# COMMAND ----------

df_px_cat_g1v2 = spark.read.format('parquet')\
    .option('header', True)\
    .option('inferSchema', True)\
    .load(f"abfss://bronze@datalakeAbhaykr07.dfs.core.windows.net/source_erp/PX_CAT_G1V2.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming cust_info file

# COMMAND ----------

df_cust_info = df_cust_info.withColumn('cst_create_date', to_date(col('cst_create_date')))\
    .withColumn('cst_id', col('cst_id').cast(IntegerType()))


# COMMAND ----------

df_cust_info.createOrReplaceTempView('cust_info')


# COMMAND ----------

df_cust_info = spark.sql('''
            SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) as cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 ELSE 'n/a'
			END AS cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				 ELSE 'n/a'
			END AS cst_gndr,
			cst_create_date
            FROM(
                SELECT*,
                ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
                FROM cust_info
                WHERE cst_id IS NOT NULL
                )t WHERE flag_last =1

''')

# COMMAND ----------

df_cust_info.write.format('delta')\
    .mode('overwrite')\
    .option('path', f"abfss://silver@datalakeAbhaykr07.dfs.core.windows.net/crm_cust_info/")\
    .saveAsTable('my_catalog.silver.crm_cust_info')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming prd_info file

# COMMAND ----------

df_prd_info = df_prd_info.withColumn('prd_id', col('prd_id').cast(IntegerType()))\
            .withColumn('prd_cost', col('prd_cost').cast(IntegerType()))\
            .withColumn('prd_start_dt', to_date(col('prd_start_dt')))\
            .withColumn('prd_end_dt', to_date(col('prd_end_dt')))\
            

# COMMAND ----------

df_prd_info.createOrReplaceTempView('prd_info')

# COMMAND ----------


df_prd_info = spark.sql('''
        SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
			prd_nm,
			COALESCE(prd_cost, 0) AS prd_cost, 
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			prd_start_dt,
			(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 ) AS prd_end_dt

        FROM prd_info
''')
    

# COMMAND ----------

df_prd_info.write.format('delta')\
    .mode('overwrite')\
    .option('path', f"abfss://silver@datalakeAbhaykr07.dfs.core.windows.net/crm_prd_info/")\
    .saveAsTable('my_catalog.silver.crm_prd_info')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Transforming  Sales_Details File

# COMMAND ----------

df_sales_details = df_sales_details.withColumn('sls_cust_id', col('sls_cust_id').cast(IntegerType()))\
                    .withColumn('sls_order_dt', to_date(col('sls_order_dt').cast(StringType()), 'yyyyMMdd'))\
                    .withColumn('sls_ship_dt', to_date(col('sls_ship_dt').cast(StringType()), 'yyyyMMdd'))\
                    .withColumn('sls_due_dt', to_date(col('sls_due_dt').cast(StringType()), 'yyyyMMdd'))\
                    .withColumn('sls_sales', col('sls_sales').cast(IntegerType()))\
                    .withColumn('sls_quantity', col('sls_quantity').cast(IntegerType()))\
                    .withColumn('sls_price', col('sls_price').cast(IntegerType()))\
                    
                   
            

# COMMAND ----------

df_sales_details.createOrReplaceTempView('sales_details')

# COMMAND ----------

df_sales_details = spark.sql('''

  	SELECT
      		sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			CASE WHEN sls_sales IS NULL or sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales  --Recalculate sales if original value is missing or incorrect
			END AS  sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity,0)
			ELSE sls_price
			END AS sls_price	--Derive price if original value is invalid
	FROM sales_details 

''')


# COMMAND ----------

df_sales_details.write.format('delta')\
        .mode('overwrite')\
        .option('path', f"abfss://silver@datalakeAbhaykr07.dfs.core.windows.net/crm_sales_details")\
        .saveAsTable('my_catalog.silver.crm_sales_details')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming cust_az12 file

# COMMAND ----------

df_cust_az12 = df_cust_az12.withColumn('BDATE', col('BDATE').cast(DateType()))

# COMMAND ----------

df_cust_az12.createOrReplaceTempView('cust_az12')

# COMMAND ----------

df_cust_az12 = spark.sql('''
        SELECT 
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid)) 
                        ELSE cid 
                    END AS cid ,
                    CASE WHEN bdate > CURRENT_DATE() THEN NULL
                        ELSE bdate
                    END AS bdate,
                    CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                        ELSE 'n/a'
                END AS gen
        FROM cust_az12
''')

# COMMAND ----------

df_cust_az12.write.format('delta')\
        .mode('overwrite')\
        .option('path', f"abfss://silver@datalakeAbhaykr07.dfs.core.windows.net/erp_cust_az12")\
        .saveAsTable('my_catalog.silver.erp_cust_az12')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming loc_a101 file

# COMMAND ----------

df_loc_a101.createOrReplaceTempView('loc_a101')

# COMMAND ----------

df_loc_a101 = spark.sql(''' 
        SELECT 
            REPLACE(cid, '-','') AS cid,
            CASE WHEN TRIM (cntry) = 'DE' THEN 'Germany'
                    WHEN TRIM (cntry) IN ('US','USA') THEN 'United States'
                    WHEN TRIM (cntry) = '' or cntry IS NULL THEN 'n/a'
                    ELSE TRIM(cntry)
            END cntry

        FROM loc_a101
''')

# COMMAND ----------

df_loc_a101.write.format('delta')\
        .mode('overwrite')\
        .option('path', f"abfss://silver@datalakeAbhaykr07.dfs.core.windows.net/erp_loc_a101")\
        .saveAsTable('my_catalog.silver.erp_loc_a101')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transforming px_cat_g1v2 file

# COMMAND ----------

df_px_cat_g1v2.write.format('delta')\
        .mode('overwrite')\
        .option('path', f"abfss://silver@datalakeAbhaykr07.dfs.core.windows.net/erp_px_cat_g1v2")\
        .saveAsTable('my_catalog.silver.erp_px_cat_g1v2')

# COMMAND ----------

