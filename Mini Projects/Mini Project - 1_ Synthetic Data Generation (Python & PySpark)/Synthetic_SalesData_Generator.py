# Databricks notebook source
# Define dbutils widgets for user input
dbutils.widgets.text("num_of_rows", "1000")
dbutils.widgets.dropdown("generation_type", "delta", ["delta", "full"])

# COMMAND ----------

# Import required libraries.
import pandas as pd
import random
from datetime import datetime, timedelta

# COMMAND ----------

# Fetch user inputs
num_of_rows = int(dbutils.widgets.get("num_of_rows"))
generation_type = dbutils.widgets.get("generation_type")

# COMMAND ----------

# PostgreSQL connection configuration
jdbc_url = "jdbc:postgresql://<HOST IP ADDRESS>:5432/<DATABASE NAME>"
jdbc_properties = {
    "user": "<USER NAME>",
    "password": "<PASSWORD>",
    "driver": "org.postgresql.Driver",
    "url": jdbc_url,
}

# COMMAND ----------

# Custom data generator functions.

def generate_customers(num_customers=20):
    """
    Generates a customer master dataset with random customer information.
    Args:
        num_customers (int): Number of customers to generate. Default is 20.
    Returns:
        pandas.DataFrame: DataFrame containing customer data.
    """
    customers = []  # Initialize an empty list to store customer data
    for i in range(1, num_customers + 1):  # Loop through to create the specified number of customers
        # Creating customer data with unique identifiers and random information
        customers.append({
            "customer_id": f"CUST{i:03}",  # Customer ID formatted as CUST001, CUST002, etc.
            "customer_name": f"Customer_{i}",  # Customer name like Customer_1, Customer_2, etc.
            "email": f"customer{i}@email.com",  # Email address using the customer number
            "phone": f"+12345678{i:03}",  # Phone number formatted as +12345678001, +12345678002, etc.
            "address": f"Address_{i}"  # Address placeholder like Address_1, Address_2, etc.
        })
    return pd.DataFrame(customers)  # Return the list of customers as a Pandas DataFrame

# COMMAND ----------

def generate_products_with_price_history(num_products=10):
    """
    Generates a product master dataset with price history.
    Args:
        num_products (int): Number of products to generate. Default is 10.
    Returns:
        tuple: Two DataFrames, one for product details and one for price history.
    """
    products = []  # Initialize a list to store product details
    price_history = []  # Initialize a list to store product price history

    # Loop through and generate data for the specified number of products
    for i in range(1, num_products + 1):
        product_id = f"PROD{i:03}"  # Product ID formatted as PROD001, PROD002, etc.
        product_name = f"Product_{i}"  # Product name like Product_1, Product_2, etc.
        base_price = round(random.uniform(10, 500), 2)  # Random base price between 10 and 500, rounded to two decimal places

        # Generate 1-3 price changes for each product (random price changes)
        num_changes = random.randint(1, 3)  # Random number of price changes (1 to 3)
        change_dates = [datetime.now() - timedelta(days=random.randint(1, 1000)) for _ in range(num_changes)]  # Generate random change dates
        change_dates = sorted(change_dates)  # Sort the dates in ascending order
        change_dates.append(datetime(9999, 12, 31))  # Append a far future date to represent the current price end date

        # Loop through price changes and generate price history data
        for j in range(len(change_dates) - 1):
            price_history.append({
                "product_id": product_id,  # Add product ID to price history
                "product_name": product_name,  # Add product name to price history
                "price": round(base_price + random.uniform(-10, 50), 2),  # Generate random price between base price and +/- 50
                "start_date": change_dates[j].strftime('%Y-%m-%d'),  # Convert change date to string (start date)
                "end_date": change_dates[j + 1].strftime('%Y-%m-%d')  # Convert next change date to string (end date)
            })

        # Store the current product details
        products.append({
            "product_id": product_id,  # Product ID
            "product_name": product_name,  # Product Name
            "current_price": price_history[-1]["price"]  # Current price is the last price in the price history
        })

    # Return product details and price history as separate DataFrames
    return pd.DataFrame(products), pd.DataFrame(price_history)

# COMMAND ----------

def generate_sales_orders(customers, products, start_date, end_date, num_of_rows, order_id_min_count=1):
    """
    Generates a sales order dataset with random sales transactions.
    
    Args:
        customers (pandas.DataFrame): DataFrame containing customer data.
        products (pandas.DataFrame): DataFrame containing product data.
        start_date (datetime): The start date of the orders.
        end_date (datetime): The end date of the orders.
        num_of_rows (int): Number of sales orders to generate.
        order_id_min_count (int): Minimum order ID to start from. Default is 1.
    
    Returns:
        pandas.DataFrame: DataFrame containing sales order details.
    """
    sales_orders = []  # Initialize an empty list to store sales orders

    date_range = pd.date_range(start=start_date, end=end_date).tolist()  # Generate a list of random order dates between start and end
    order_id_counter = order_id_min_count  # Start order ID counter from the given minimum count

    # Loop through and generate the specified number of sales orders
    for _ in range(num_of_rows):
        customer = random.choice(customers["customer_id"].tolist())  # Randomly select a customer
        order_date = random.choice(date_range)  # Randomly select an order date from the date range
        order_id = f"ORD{order_id_counter:06}"  # Generate order ID like ORD000001, ORD000002, etc.
        products_count = random.randint(6, len(products))  # Randomly choose the number of products in the order (6 to max number of products)
        product_ids = random.sample(products["product_id"].tolist(), products_count)  # Randomly select products for the order

        # Loop through selected products to generate order details
        for product in product_ids:
            quantity = random.randint(1, 5)  # Randomly select quantity between 1 and 5
            price = products.loc[products["product_id"] == product, "current_price"].values[0]  # Get the current price of the product
            sales_orders.append({
                "order_id": order_id,  # Order ID
                "customer_id": customer,  # Customer ID
                "product_id": product,  # Product ID
                "quantity": quantity,  # Quantity of product in the order
                "order_date": order_date.strftime('%Y-%m-%d'),  # Convert order date to string format
                "total_price": round(quantity * price, 2)  # Calculate total price as quantity * unit price
            })
        order_id_counter += 1  # Increment the order ID counter for the next order

    # Return sales order data as a Pandas DataFrame
    return pd.DataFrame(sales_orders)

# COMMAND ----------

# Main script logic
if generation_type.lower() == 'delta':
    # If the generation type is 'delta', read existing sales orders from the database
    existing_sales_orders_df = spark.read.format("jdbc").options(**jdbc_properties).option("query", "SELECT order_id FROM sales_orders").load()
    max_order_id = existing_sales_orders_df.toPandas()['order_id'].apply(lambda val: int(str(val).replace("ORD", ""))).values.max()

    # Define start and end dates for generating data
    start_date = datetime.now() - timedelta(days=2)  # 2 days ago
    end_date = datetime.now()  # Current date
    order_id_min_count = max_order_id  # Use the maximum order ID from existing data
else:
    # If the generation type is 'full', generate data for the past 3 years
    start_date = datetime.now() - timedelta(days=3 * 365)  # 3 years ago
    end_date = datetime.now() - timedelta(days=2)  # 2 days ago
    order_id_min_count = 1  # Start order ID from 1

# COMMAND ----------

# Generate data
customers_df = generate_customers()  # Generate customer data
products_df, price_history_df = generate_products_with_price_history()  # Generate product and price history data
sales_orders_df = generate_sales_orders(customers_df, products_df, start_date, end_date, num_of_rows, order_id_min_count)  # Generate sales orders

# COMMAND ----------

# Convert Pandas DataFrames to Spark DataFrames
spark_customers_df = spark.createDataFrame(customers_df)  # Convert customer data to Spark DataFrame
spark_products_df = spark.createDataFrame(price_history_df)  # Convert price history data to Spark DataFrame
spark_sales_orders_df = spark.createDataFrame(sales_orders_df)  # Convert sales orders data to Spark DataFrame

# COMMAND ----------

# Display the generated data in Databricks
spark_customers_df.display()  # Display customer data

# COMMAND ----------

spark_products_df.display()  # Display product price history data

# COMMAND ----------

spark_sales_orders_df.display()  # Display sales orders data

# COMMAND ----------

# Insert new data into PostgreSQL
if generation_type.lower() == 'delta':
    # For delta generation type, read existing data and insert only new rows
    existing_customer_master_df = spark.read.format("jdbc").options(**jdbc_properties).option("query", "SELECT * FROM customer_master").load()
    existing_product_master_df = spark.read.format("jdbc").options(**jdbc_properties).option("query", "SELECT * FROM product_master").load()
    existing_sales_orders_df = spark.read.format("jdbc").options(**jdbc_properties).option("query", "SELECT * FROM sales_orders").load()

    # Perform left_anti join to find new records not yet inserted
    latest_customer_master_df = spark_customers_df.join(existing_customer_master_df, on='customer_id', how='left_anti')
    latest_product_master_df = spark_products_df.join(existing_product_master_df, on=['product_id', 'start_date', 'end_date'], how='left_anti')
    latest_sales_orders_df = spark_sales_orders_df.join(existing_sales_orders_df, on=['order_id', 'order_date'], how='left_anti')

    # Append new records to PostgreSQL
    latest_customer_master_df.write.jdbc(url=jdbc_url, table="customer_master", mode="append", properties=jdbc_properties)
    latest_product_master_df.write.jdbc(url=jdbc_url, table="product_master", mode="append", properties=jdbc_properties)
    latest_sales_orders_df.write.jdbc(url=jdbc_url, table="sales_orders", mode="append", properties=jdbc_properties)
else:
    # For full generation type, overwrite the entire table in PostgreSQL
    spark_customers_df.write.jdbc(url=jdbc_url, table="customer_master", mode="overwrite", properties=jdbc_properties)
    spark_products_df.write.jdbc(url=jdbc_url, table="product_master", mode="overwrite", properties=jdbc_properties)
    spark_sales_orders_df.write.jdbc(url=jdbc_url, table="sales_orders", mode="overwrite", properties=jdbc_properties)

# COMMAND ----------

# Exit the notebook after successful execution
dbutils.notebook.exit("Success")  # Exit Databricks notebook with success message