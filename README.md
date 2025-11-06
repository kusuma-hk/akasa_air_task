# akasa_air_task
Akasa Air – Data Engineer Assignment 1

# Akasa Air — Data Engineer Assignment
**Author:** Kusuma H K 

---

## Assignment summary
This repository contains a solution to process and analyze customer and order data from multiple sources (CSV and XML) using both database table and data-frame (in-memory) approaches. 

---

## To implement 
- Two approaches to process the data:
  1. **A. TABLE-BASED APPROACH** 
    - Data Cleaning & Loading
    - Querying for KPIs

  2. **B. IN-MEMORY APPROACH** 
    - Data Cleaning
    - KPI Functions
---

## KPIs delivered
1. **Repeat Customers** - customers with more than one order.  
2. **Monthly Order Trends** -  Aggregate orders by month to observe trends.  
3. **Regional Revenue** - Sum of total_amount grouped by region.  
4. **Top Customers (Last 30 Days)** — Rank customers by total spend in the last 30 days.

---

## Folder structure 

data/ #input files here (customers CSV, orders XML)
src/ #ETL scripts (in-memory and SQL)
outputs/ # generated KPI CSVs (repeat_customers.csv, monthly_trends.csv, etc)
requirements.txt- # Python dependencies (to be installed)
.gitignore- # ignored files (venv, outputs, .env, etc.)