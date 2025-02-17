import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, max as Fmax, year, month, lit, concat, lpad
from pyspark.sql import DataFrame
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder 
    .appName(Incremental PowerBI Visualizations) 
    .getOrCreate()

# JDBC connection details
jdbc_url = jdbcmysqldatabase1.cvoeocy4a6iy.us-east-1.rds.amazonaws.xxxxxxxxxxxx
properties = {
    user admin,
    password admin123,
    driver com.mysql.cj.jdbc.Driver
}

# S3 base path for saving visualizations
s3_base_path = s3visuals-11111tableau_visualizations

# Checkpoint directory for saving last processed timestamps
checkpoint_dir = dbfstmptableau_cdc_checkpoints
os.makedirs(checkpoint_dir, exist_ok=True)

def get_last_processed_timestamp(table str)
    Fetch the last processed timestamp for a table.
    file_path = f{checkpoint_dir}{table}_last_processed.txt
    if os.path.exists(file_path)
        with open(file_path, r) as file
            return file.read().strip()
    return 1970-01-01 000000

def save_last_processed_timestamp(table str, timestamp datetime)
    Save the last processed timestamp for a table.
    file_path = f{checkpoint_dir}{table}_last_processed.txt
    with open(file_path, w) as file
        # Convert the datetime object to a string before writing
        file.write(timestamp.strftime('%Y-%m-%d %H%M%S'))

def read_incremental_data(table str, timestamp_col str)
    Read only new or updated data from a MySQL table.
    last_timestamp = get_last_processed_timestamp(table)
    
    # Ensure that we are comparing against a valid timestamp
    if not last_timestamp
        last_timestamp = 1970-01-01 000000  # Fallback to a default value if last timestamp is invalid
    
    # Construct SQL query to fetch data that has been updated after the last timestamp
  
    if table ==order_details
         query = f
    (SELECT o.,od.{timestamp_col} FROM {table} as o join orders od  on o.order_id=od.order_id
    WHERE IFNULL(od.{timestamp_col}, '1970-01-01 000000')  '{last_timestamp}') AS subquery
    
    else
          query = f
    (SELECT  FROM {table} 
    WHERE IFNULL({timestamp_col}, '1970-01-01 000000')  '{last_timestamp}') AS subquery
    
        
    # Load the incremental data from MySQL into a Spark DataFrame
    df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)
    
    if not df.isEmpty()
        # Get the maximum timestamp in the current data
        max_timestamp = df.agg(Fmax(col(timestamp_col)).alias(max_ts)).collect()[0][max_ts]
        
        # Ensure we don't save invalid timestamp values
        if max_timestamp
            save_last_processed_timestamp(table, max_timestamp)
    
    return df

# Load Incremental Data
customers_df = read_incremental_data(customers, created_at)
orders_df = read_incremental_data(orders, timestamp)
order_details_df = read_incremental_data(order_details, timestamp)
payments_df = read_incremental_data(payments, payment_date)

# Define functions for each visualization

def total_revenue_by_customer(orders_df)
    return orders_df.groupBy(customer_id).agg(sum(total_amount).alias(total_revenue))

def monthly_sales_trend(orders_df)
    return orders_df.withColumn(month, concat(year(order_date), lit(-), lpad(month(order_date), 2, 0))) 
                    .groupBy(month).agg(sum(total_amount).alias(total_sales))

def product_sales_summary(order_details_df)
    return order_details_df.groupBy(product_id, product_name).agg(
        sum(quantity).alias(total_quantity),
        sum(col(quantity)  col(price_per_unit)).alias(total_revenue)
    )

def payment_method_usage(payments_df)
    return payments_df.groupBy(payment_method).agg(count().alias(usage_count))

def average_order_value(orders_df)
    return orders_df.withColumn(month, concat(year(order_date), lit(-), lpad(month(order_date), 2, 0))) 
                    .groupBy(month).agg(avg(total_amount).alias(average_order_value))

def top_selling_products(order_details_df)
    return product_sales_summary(order_details_df).orderBy(col(total_revenue).desc()).limit(10)

def order_status_breakdown(orders_df)
    return orders_df.groupBy(order_status).agg(count().alias(status_count))

def sales_by_geography(customers_df, orders_df)
    return customers_df.join(orders_df, customer_id).groupBy(city, state, country).agg(
        sum(total_amount).alias(total_sales)
    )

def customer_lifetime_value(orders_df)
    return orders_df.groupBy(customer_id).agg(sum(total_amount).alias(lifetime_value))

def delayed_orders_analysis(orders_df)
    delayed_orders_df = orders_df.filter(col(order_status) == Delayed)
    return delayed_orders_df.groupBy(order_status).agg(count().alias(delayed_count))

# Function to write DataFrame to S3 as Parquet
def write_to_s3(df DataFrame, path str)
    df.write.mode(overwrite).parquet(f{s3_base_path}{path})

# Execute Visualizations and write results to S3
visualizations = {
    revenue_by_customer total_revenue_by_customer(orders_df),
    monthly_sales_trend monthly_sales_trend(orders_df),
    product_sales_summary product_sales_summary(order_details_df),
    payment_method_usage payment_method_usage(payments_df),
    average_order_value average_order_value(orders_df),
    top_selling_products top_selling_products(order_details_df),
    order_status_breakdown order_status_breakdown(orders_df),
    sales_by_geography sales_by_geography(customers_df, orders_df),
    customer_lifetime_value customer_lifetime_value(orders_df),
    delayed_orders_analysis delayed_orders_analysis(orders_df)
}

# Write each result to S3
for name, result_df in visualizations.items()
    write_to_s3(result_df, name)









def total_revenue_by_customer(orders_df):
    return customers_df

def monthly_sales_trend(orders_df):
    return orders_df

def product_sales_summary(order_details_df):
    return order_details_df

def payment_method_usage(payments_df):
    return payments_df

def write_to_s3(df: DataFrame, path: str):
    df.write.mode("overwrite").parquet(f"{s3_base_path}/{path}")

# Execute Visualizations and write results to S3
visualizations = {
    "revenue_by_customer": total_revenue_by_customer(orders_df),
    "monthly_sales_trend": monthly_sales_trend(orders_df),
    "product_sales_summary": product_sales_summary(order_details_df),
    "payment_method_usage": payment_method_usage(payments_df)
}

# Write each result to S3
for name, result_df in visualizations.items():
    result_df.show(2)
    write_to_s3(result_df, name)
