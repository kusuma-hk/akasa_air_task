import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import os
import logging

# CONFIG 
DATA_DIR = "data"
OUT_DIR = "outputs"
os.makedirs(OUT_DIR, exist_ok=True)

logging.basicConfig(
    filename="etl_inmemory.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# DATA LOAD and CLEAN 
def read_customers():
    """Load and clean customers.csv"""
    path = os.path.join(DATA_DIR, "task_DE_new_customers.csv")
    df = pd.read_csv(path, dtype=str)
    df.columns = [c.strip().lower() for c in df.columns]
    df.drop_duplicates(subset=["mobile_number"], inplace=True)
    df.fillna({"region": "Unknown"}, inplace=True)
    return df

def read_orders():
    """Parse and clean orders.xml"""
    path = os.path.join(DATA_DIR, "task_DE_new_orders.xml")
    tree = ET.parse(path)
    root = tree.getroot()

    records = []
    for order in root.findall("order"):
        try:
            records.append({
                "order_id": order.findtext("order_id"),
                "mobile_number": order.findtext("mobile_number"),
                "order_date_time": order.findtext("order_date_time"),
                "sku_id": order.findtext("sku_id"),
                "sku_count": int(order.findtext("sku_count") or 0),
                "total_amount": float(order.findtext("total_amount") or 0.0)
            })
        except Exception as e:
            logging.warning(f"Skipping malformed order: {e}")

    df = pd.DataFrame(records)
    df["order_date_time"] = pd.to_datetime(df["order_date_time"], errors="coerce", utc=True)
    df.dropna(subset=["order_id", "mobile_number"], inplace=True)
    return df


#  KPI FUNCTIONS 
def kpi_repeat_customers(orders):
    """Customers with >1 order"""
    out = orders.groupby("mobile_number")["order_id"].count().reset_index(name="order_count")
    return out[out["order_count"] > 1]


def kpi_monthly_trends(orders):
    """Total orders per month"""
    orders["month"] = orders["order_date_time"].dt.strftime("%Y-%m")
    out = orders.groupby("month")["order_id"].count().reset_index(name="total_orders")
    return out.sort_values("month")


def kpi_regional_revenue(customers, orders):
    """Revenue grouped by customer region"""
    merged = orders.merge(customers, on="mobile_number", how="left")
    out = (
        merged.groupby("region")["total_amount"]
        .sum()
        .reset_index(name="total_revenue")
        .sort_values("total_revenue", ascending=False)
    )
    return out

def kpi_top_spenders(customers, orders, days=30):
    """Top 10 spenders in the last N days"""
    cutoff = datetime.now().astimezone() - timedelta(days=days)
    recent = orders[orders["order_date_time"] >= cutoff]
    merged = recent.merge(customers, on="mobile_number", how="left")
    out = (
        merged.groupby(["customer_name", "mobile_number"])["total_amount"]
        .sum()
        .reset_index(name="total_spent")
        .sort_values("total_spent", ascending=False)
        .head(10)
    )
    return out

# MAIN EXECUTION 
def main():
    try:
        customers = read_customers()
        orders = read_orders()

        # Compute KPIs
        repeat = kpi_repeat_customers(orders)
        monthly = kpi_monthly_trends(orders)
        regional = kpi_regional_revenue(customers, orders)
        top = kpi_top_spenders(customers, orders)

        # Export results
        repeat.to_csv(f"{OUT_DIR}/inmemory_repeat_customers.csv", index=False)
        monthly.to_csv(f"{OUT_DIR}/inmemory_monthly_trends.csv", index=False)
        regional.to_csv(f"{OUT_DIR}/inmemory_regional_revenue.csv", index=False)
        top.to_csv(f"{OUT_DIR}/inmemory_top_spenders_30days.csv", index=False)

        print("KPIs generated successfully! Inside the 'outputs/' folder.")
        logging.info("In-Memory ETL completed successfully.")

    except Exception as e:
        print(f"Error during In-Memory ETL: {e}")
        logging.error(f"ETL failed: {e}")

if __name__ == "__main__":
    main()
