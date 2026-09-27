import os
import pandas as pd
import sys

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import config

def validate_datasets():
    raw_dir = config.RAW_DIR
    quarantine_dir = os.path.join(config.DATA_DIR, 'quarantine')
    os.makedirs(quarantine_dir, exist_ok=True)
    
    files = [
        "customers.csv",
        "products.csv",
        "orders.csv",
        "order_items.csv",
        "clickstream.csv",
        "support_tickets.csv"
    ]
    
    # 1. Check existence
    for f in files:
        if not os.path.exists(os.path.join(raw_dir, f)):
            print(f"ERROR: File {f} is missing in {raw_dir}")
            return
    print("All required CSV files are present.")
    
    # 2. Load and validate schemas / nulls / duplicates
    dfs = {}
    
    # customers
    df_customers = pd.read_csv(os.path.join(raw_dir, "customers.csv"))
    dfs["customers"] = df_customers
    dup_cust = df_customers.duplicated(subset=['customer_id']).sum()
    null_cust = df_customers['customer_id'].isnull().sum()
    print(f"[Customers] Rows: {len(df_customers)}, Duplicates: {dup_cust}, Null IDs: {null_cust}")
    
    # products
    df_products = pd.read_csv(os.path.join(raw_dir, "products.csv"))
    dfs["products"] = df_products
    dup_prod = df_products.duplicated(subset=['product_id']).sum()
    null_prod = df_products['product_id'].isnull().sum()
    print(f"[Products] Rows: {len(df_products)}, Duplicates: {dup_prod}, Null IDs: {null_prod}")
    
    # orders
    df_orders = pd.read_csv(os.path.join(raw_dir, "orders.csv"))
    dfs["orders"] = df_orders
    dup_ord = df_orders.duplicated(subset=['order_id']).sum()
    null_ord = df_orders['order_id'].isnull().sum()
    # Check numeric order_total
    df_orders['order_total'] = pd.to_numeric(df_orders['order_total'], errors='coerce')
    invalid_totals = df_orders['order_total'].isnull().sum()
    print(f"[Orders] Rows: {len(df_orders)}, Duplicates: {dup_ord}, Null IDs: {null_ord}, Invalid Totals: {invalid_totals}")
    
    # order_items
    df_items = pd.read_csv(os.path.join(raw_dir, "order_items.csv"))
    dfs["order_items"] = df_items
    dup_item = df_items.duplicated(subset=['order_item_id']).sum()
    null_item = df_items['order_item_id'].isnull().sum()
    df_items['line_total'] = pd.to_numeric(df_items['line_total'], errors='coerce')
    invalid_lines = df_items['line_total'].isnull().sum()
    print(f"[Order Items] Rows: {len(df_items)}, Duplicates: {dup_item}, Null IDs: {null_item}, Invalid Line Totals: {invalid_lines}")
    
    # clickstream
    df_click = pd.read_csv(os.path.join(raw_dir, "clickstream.csv"))
    dfs["clickstream"] = df_click
    dup_click = df_click.duplicated(subset=['event_id']).sum()
    null_click = df_click['event_id'].isnull().sum()
    invalid_dates_click = pd.to_datetime(df_click['event_timestamp'], errors='coerce').isnull().sum()
    print(f"[Clickstream] Rows: {len(df_click)}, Duplicates: {dup_click}, Null IDs: {null_click}, Invalid Dates: {invalid_dates_click}")
    
    # support_tickets
    df_tickets = pd.read_csv(os.path.join(raw_dir, "support_tickets.csv"))
    dfs["support_tickets"] = df_tickets
    dup_tick = df_tickets.duplicated(subset=['ticket_id']).sum()
    null_tick = df_tickets['ticket_id'].isnull().sum()
    # Check resolution timestamp
    # Assuming columns created_at, resolved_at
    resolved = df_tickets.dropna(subset=['resolved_at'])
    invalid_resolved_dates = pd.to_datetime(resolved['resolved_at'], errors='coerce').isnull().sum()
    print(f"[Support Tickets] Rows: {len(df_tickets)}, Duplicates: {dup_tick}, Null IDs: {null_tick}, Invalid Resolved Dates: {invalid_resolved_dates}")
    
    # 3. Foreign key validation
    valid_customers = set(df_customers['customer_id'].dropna())
    valid_products = set(df_products['product_id'].dropna())
    valid_orders = set(df_orders['order_id'].dropna())
    
    fk_orders_cust = ~df_orders['customer_id'].isin(valid_customers)
    print(f"Foreign Key Violations - Orders -> Customers: {fk_orders_cust.sum()}")
    
    fk_items_ord = ~df_items['order_id'].isin(valid_orders)
    fk_items_prod = ~df_items['product_id'].isin(valid_products)
    print(f"Foreign Key Violations - Order Items -> Orders: {fk_items_ord.sum()}")
    print(f"Foreign Key Violations - Order Items -> Products: {fk_items_prod.sum()}")
    
    # Save quarantined records
    if fk_orders_cust.sum() > 0:
        quarantined_orders = df_orders[fk_orders_cust]
        quarantined_orders.to_csv(os.path.join(quarantine_dir, 'quarantined_orders.csv'), index=False)
        print("Saved quarantined orders.")
        
    print("\nDataset validation completed successfully.")

if __name__ == "__main__":
    validate_datasets()
