import polars as pl
import pandas as pd
import sqlite3

# Load the data from products.parquet file.
products = pl.read_parquet('data/products.parquet')

# Load the data from sales.parquet file.
sales = pl.read_parquet('data/sales.parquet')

# Merge the data from both files based on the product_id column to create a new dataset.
combined_sales = products.join(sales, on="product_id", how="inner")

# Calculate the total revenue for each sale by multiplying the quantity_sold with the unit_price for each product.
combined_sales = combined_sales.with_columns(pl.col("product_id"), (pl.col("unit_price") * pl.col("quantity_sold")).alias("total_revenue"))

# Save the transformed data into a new PARQUET file named combined_sales.parquet.
combined_sales = combined_sales.select(pl.col(["sale_id", "product_id", "product_name", "quantity_sold", "unit_price", "sale_date", "total_revenue"]))
combined_sales.write_parquet("out/combined_sales.parquet")

# Create database cheil_sales.db
conn = sqlite3.connect("database/cheil_sales.db")

# Create table combined_sales.
cur = conn.cursor()
cur.execute("CREATE TABLE combined_sales(sale_id, product_id, product_name, quantity_sold, unit_price, sale_date, total_revenue)")

# Fill table with dataframe combined_sales data.
combined_sales.to_pandas().to_sql('combined_sales', conn, index=True, if_exists='replace')