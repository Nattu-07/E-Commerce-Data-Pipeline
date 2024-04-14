import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node dim_products
dim_products_node1713023950891 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-891377251081-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "ecommerce_data.dim_products", "connectionName": "redshift-connection-aws-de"}, transformation_ctx="dim_products_node1713023950891")

# Script generated for node S3 Transactions
S3Transactions_node1713023708152 = glueContext.create_dynamic_frame.from_catalog(database="sales_db", table_name="transactions", transformation_ctx="S3Transactions_node1713023708152")

# Script generated for node dim_customers
dim_customers_node1713023998618 = glueContext.create_dynamic_frame.from_options(connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-891377251081-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "ecommerce_data.dim_customers", "connectionName": "redshift-connection-aws-de"}, transformation_ctx="dim_customers_node1713023998618")

# Script generated for node Renamed keys for dim_products
Renamedkeysfordim_products_node1713024251496 = ApplyMapping.apply(frame=dim_products_node1713023950891, mappings=[("product_id", "int", "dim_product_id", "int"), ("product_name", "string", "product_name", "string"), ("category", "string", "category", "string"), ("price", "decimal", "price", "decimal"), ("supplier_id", "int", "supplier_id", "int")], transformation_ctx="Renamedkeysfordim_products_node1713024251496")

# Script generated for node trans_prod_join
trans_prod_join_node1713024090955 = Join.apply(frame1=S3Transactions_node1713023708152, frame2=Renamedkeysfordim_products_node1713024251496, keys1=["product_id"], keys2=["dim_product_id"], transformation_ctx="trans_prod_join_node1713024090955")

# Script generated for node Renamed keys for dim_customers
Renamedkeysfordim_customers_node1713024532220 = ApplyMapping.apply(frame=dim_customers_node1713023998618, mappings=[("customer_id", "int", "dim_customer_id", "int"), ("first_name", "string", "first_name", "string"), ("last_name", "string", "last_name", "string"), ("email", "string", "email", "string"), ("membership_level", "string", "membership_level", "string")], transformation_ctx="Renamedkeysfordim_customers_node1713024532220")

# Script generated for node trans_prod_cust_join
trans_prod_cust_join_node1713024508457 = Join.apply(frame1=trans_prod_join_node1713024090955, frame2=Renamedkeysfordim_customers_node1713024532220, keys1=["customer_id"], keys2=["dim_customer_id"], transformation_ctx="trans_prod_cust_join_node1713024508457")

# Script generated for node transformed_transactions
SqlQuery2129 = '''
select *,
case when total_price <= 100 then "Small"
when total_price <= 300 then "Medium"
else "Large" end as transaction_type
from
(select *,
round(price * quantity,2) as total_price
from transactions)
'''
transformed_transactions_node1713024802022 = sparkSqlQuery(glueContext, query = SqlQuery2129, mapping = {"transactions":trans_prod_cust_join_node1713024508457}, transformation_ctx = "transformed_transactions_node1713024802022")

# Script generated for node final_schema
final_schema_node1713025129475 = ApplyMapping.apply(frame=transformed_transactions_node1713024802022, mappings=[("first_name", "string", "first_name", "string"), ("product_id", "bigint", "product_id", "int"), ("product_name", "string", "product_name", "string"), ("membership_level", "string", "membership_level", "string"), ("customer_id", "bigint", "customer_id", "int"), ("supplier_id", "int", "supplier_id", "int"), ("category", "string", "category", "string"), ("transaction_date", "string", "transaction_date", "date"), ("payment_type", "string", "payment_type", "string"), ("transaction_id", "string", "transaction_id", "string"), ("quantity", "bigint", "quantity", "int"), ("status", "string", "status", "string"), ("email", "string", "customer_email", "string"), ("last_name", "string", "last_name", "string"), ("total_price", "decimal", "total_price", "decimal"), ("transaction_type", "string", "transaction_type", "string")], transformation_ctx="final_schema_node1713025129475")

# Script generated for node Redshift Fact Trans Load
RedshiftFactTransLoad_node1713025826329 = glueContext.write_dynamic_frame.from_options(frame=final_schema_node1713025129475, connection_type="redshift", connection_options={"postactions": "BEGIN; MERGE INTO ecommerce_data.fact_transactions USING ecommerce_data.fact_transactions_temp_fjihzn ON fact_transactions.transaction_id = fact_transactions_temp_fjihzn.transaction_id WHEN MATCHED THEN UPDATE SET first_name = fact_transactions_temp_fjihzn.first_name, product_id = fact_transactions_temp_fjihzn.product_id, product_name = fact_transactions_temp_fjihzn.product_name, membership_level = fact_transactions_temp_fjihzn.membership_level, customer_id = fact_transactions_temp_fjihzn.customer_id, supplier_id = fact_transactions_temp_fjihzn.supplier_id, category = fact_transactions_temp_fjihzn.category, transaction_date = fact_transactions_temp_fjihzn.transaction_date, payment_type = fact_transactions_temp_fjihzn.payment_type, transaction_id = fact_transactions_temp_fjihzn.transaction_id, quantity = fact_transactions_temp_fjihzn.quantity, status = fact_transactions_temp_fjihzn.status, customer_email = fact_transactions_temp_fjihzn.customer_email, last_name = fact_transactions_temp_fjihzn.last_name, total_price = fact_transactions_temp_fjihzn.total_price, transaction_type = fact_transactions_temp_fjihzn.transaction_type WHEN NOT MATCHED THEN INSERT VALUES (fact_transactions_temp_fjihzn.first_name, fact_transactions_temp_fjihzn.product_id, fact_transactions_temp_fjihzn.product_name, fact_transactions_temp_fjihzn.membership_level, fact_transactions_temp_fjihzn.customer_id, fact_transactions_temp_fjihzn.supplier_id, fact_transactions_temp_fjihzn.category, fact_transactions_temp_fjihzn.transaction_date, fact_transactions_temp_fjihzn.payment_type, fact_transactions_temp_fjihzn.transaction_id, fact_transactions_temp_fjihzn.quantity, fact_transactions_temp_fjihzn.status, fact_transactions_temp_fjihzn.customer_email, fact_transactions_temp_fjihzn.last_name, fact_transactions_temp_fjihzn.total_price, fact_transactions_temp_fjihzn.transaction_type); DROP TABLE ecommerce_data.fact_transactions_temp_fjihzn; END;", "redshiftTmpDir": "s3://aws-glue-assets-891377251081-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "ecommerce_data.fact_transactions_temp_fjihzn", "connectionName": "redshift-connection-aws-de", "preactions": "CREATE TABLE IF NOT EXISTS ecommerce_data.fact_transactions (first_name VARCHAR, product_id INTEGER, product_name VARCHAR, membership_level VARCHAR, customer_id INTEGER, supplier_id INTEGER, category VARCHAR, transaction_date DATE, payment_type VARCHAR, transaction_id VARCHAR, quantity INTEGER, status VARCHAR, customer_email VARCHAR, last_name VARCHAR, total_price DECIMAL, transaction_type VARCHAR); DROP TABLE IF EXISTS ecommerce_data.fact_transactions_temp_fjihzn; CREATE TABLE ecommerce_data.fact_transactions_temp_fjihzn (first_name VARCHAR, product_id INTEGER, product_name VARCHAR, membership_level VARCHAR, customer_id INTEGER, supplier_id INTEGER, category VARCHAR, transaction_date DATE, payment_type VARCHAR, transaction_id VARCHAR, quantity INTEGER, status VARCHAR, customer_email VARCHAR, last_name VARCHAR, total_price DECIMAL, transaction_type VARCHAR);"}, transformation_ctx="RedshiftFactTransLoad_node1713025826329")

job.commit()