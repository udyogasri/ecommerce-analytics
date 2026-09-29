import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

# Create dirs
base_path = r"C:\Users\allub\ecommerce-analytics\data\local_test\lakehouse\gold"
os.makedirs(os.path.join(base_path, "facts", "fact_sales"), exist_ok=True)
os.makedirs(os.path.join(base_path, "dimensions", "dim_customers"), exist_ok=True)
os.makedirs(os.path.join(base_path, "dimensions", "dim_date"), exist_ok=True)

# 1. Generate dim_date
dates = pd.date_range(start="2022-01-01", end="2024-12-31")
dim_date = pd.DataFrame({
    "full_date": dates,
    "date_key": dates.strftime('%Y%m%d').astype(int),
    "day": dates.day,
    "day_of_week": dates.dayofweek + 1,
    "month": dates.month,
    "year": dates.year,
    "quarter": dates.quarter
})
dim_date.to_parquet(os.path.join(base_path, "dimensions", "dim_date", "dim_date.parquet"), index=False)

# 2. Generate dim_customers
customers = pd.DataFrame({
    "customer_key": [f"C{i}" for i in range(1, 101)],
    "customer_id": [f"C{i}" for i in range(1, 101)],
    "customer_name": [f"Customer {i}" for i in range(1, 101)],
    "customer_segment": np.random.choice(["Retail", "Wholesale", "Corporate"], 100),
    "city": np.random.choice(["New York", "London", "Tokyo", "Paris"], 100)
})
customers.to_parquet(os.path.join(base_path, "dimensions", "dim_customers", "dim_customers.parquet"), index=False)

# 3. Generate fact_sales
np.random.seed(42)
n_sales = 5000
random_dates = np.random.choice(dates, n_sales)
quantities = np.random.randint(1, 5, n_sales)
prices = np.random.uniform(10.0, 500.0, n_sales)

fact_sales = pd.DataFrame({
    "order_item_id": [f"ITEM-{i}" for i in range(1, n_sales + 1)],
    "order_id": [f"ORD-{i//3}" for i in range(1, n_sales + 1)],
    "customer_key": np.random.choice(customers["customer_key"], n_sales),
    "product_key": [f"P{np.random.randint(1, 50)}" for _ in range(n_sales)],
    "order_date_key": pd.to_datetime(random_dates).strftime('%Y%m%d').astype(int),
    "order_status": np.random.choice(["Completed", "Shipped", "Cancelled", "Pending"], n_sales, p=[0.7, 0.2, 0.05, 0.05]),
    "quantity": quantities,
    "unit_price": prices,
    "discount_pct": np.random.choice([0.0, 5.0, 10.0, 15.0], n_sales)
})

fact_sales["gross_sales"] = fact_sales["quantity"] * fact_sales["unit_price"]
fact_sales["discount_amount"] = fact_sales["gross_sales"] * (fact_sales["discount_pct"] / 100)
fact_sales["net_sales"] = fact_sales["gross_sales"] - fact_sales["discount_amount"]
fact_sales["recognized_sales"] = np.where(fact_sales["order_status"].isin(["Completed", "Shipped"]), fact_sales["net_sales"], 0.0)

fact_sales.to_parquet(os.path.join(base_path, "facts", "fact_sales", "fact_sales.parquet"), index=False)

print("SUCCESS: Parquet files generated.")
