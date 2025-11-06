import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import os
import logging
from db_connection import get_engine  

# CONFIGURATION and LOG INFO

OUT_DIR = "outputs"
os.makedirs(OUT_DIR, exist_ok=True)

logging.basicConfig(
    filename="etl_database.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# STEP 1: DATA EXTRACTION

def read_customers(csv_path="data/task_DE_new_customers.csv"):
    """Read and clean customers CSV."""
    df = pd.read_csv(csv_path, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]
    df.drop_duplicates(subset=["mobile_number"], inplace=True)
    logging.info(f"Loaded {len(df)} customers from CSV.")
    return df


def read_orders(xml_path="data/task_DE_new_orders.xml"):
    """Parse XML into a DataFrame."""
    tree = ET.parse(xml_path)
    root = tree.getroot()
    data = []

    for order in root.findall("order"):
        try:
            data.append({
                "order_id": order.findtext("order_id"),
                "mobile_number": order.findtext("mobile_number"),
                "order_date_time": order.findtext("order_date_time"),
                "sku_id": order.findtext("sku_id"),
                "sku_count": int(order.findtext("sku_count") or 0),
                "total_amount": float(order.findtext("total_amount") or 0)
            })
        except Exception as e:
            logging.warning(f"Skipping incorrectly structured or broken order: {e}")

    df = pd.DataFrame(data)
    df["order_date_time"] = pd.to_datetime(df["order_date_time"], errors="coerce", utc=True)
    df.dropna(subset=["order_id", "mobile_number"], inplace=True)

    logging.info(f"Parsed {len(df)} valid orders from XML.")
    return df

# STEP 2: LOAD INTO DATABASE

def load_to_db(engine, customers, orders):
    """Create and load database tables."""
    customers.to_sql("customers", engine, if_exists="replace", index=False)
    orders.to_sql("orders", engine, if_exists="replace", index=False)
    logging.info("Loaded customers and orders into database.")

# STEP 3: COMPUTE KPIs with SQL QUERIES

def compute_kpis(engine):
    """Run SQL queries to compute key performance indicators."""

    # 1️. Repeat Customers
    repeat = pd.read_sql_query("""
        SELECT mobile_number, COUNT(order_id) AS order_count
        FROM orders
        GROUP BY mobile_number
        HAVING order_count > 1;
    """, engine)
    repeat.to_csv(f"{OUT_DIR}/repeat_customers.csv", index=False)

    # 2️. Monthly Order Trends (MySQL compatible)
    monthly = pd.read_sql_query("""
        SELECT DATE_FORMAT(order_date_time, '%Y-%m') AS month, COUNT(order_id) AS total_orders
        FROM orders
        GROUP BY month
        ORDER BY month;
    """, engine)
    monthly.to_csv(f"{OUT_DIR}/monthly_order_trends.csv", index=False)

    # 3️. Regional Revenue
    regional = pd.read_sql_query("""
        SELECT c.region, SUM(o.total_amount) AS total_revenue
        FROM orders o
        JOIN customers c ON o.mobile_number = c.mobile_number
        GROUP BY c.region
        ORDER BY total_revenue DESC;
    """, engine)
    regional.to_csv(f"{OUT_DIR}/regional_revenue.csv", index=False)

    # 4️. Top Customers (Last 30 Days)
    cutoff = (datetime.now().astimezone() - timedelta(days=30)).isoformat()
    top_spenders = pd.read_sql_query(f"""
        SELECT c.customer_name, c.mobile_number, SUM(o.total_amount) AS total_spent
        FROM orders o
        JOIN customers c ON o.mobile_number = c.mobile_number
        WHERE o.order_date_time >= '{cutoff}'
        GROUP BY c.customer_name, c.mobile_number
        ORDER BY total_spent DESC
        LIMIT 10;
    """, engine)
    top_spenders.to_csv(f"{OUT_DIR}/top_spenders_30days.csv", index=False)

    logging.info("All KPIs computed successfully.")

# STEP 4: MAIN EXECUTION

def main():
    try:
        customers = read_customers("data/task_DE_new_customers.csv")
        orders = read_orders("data/task_DE_new_orders.xml")

        engine = get_engine().connect()
        load_to_db(engine, customers, orders)
        compute_kpis(engine)
        engine.close()

        print(" KPIs generated successfully! Inside the 'outputs/' folder.")
        logging.info("ETL process completed successfully.")

    except Exception as e:
        logging.error(f"ETL failed: {e}")
        print(f"Error during ETL: {e}. Check logs for details.")

# MAIN

if __name__ == "__main__":
    main()
