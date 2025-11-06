# Akasa Air – Data Engineer Assignment 1

**Author:** Kusuma H K 

---

## Assignment summary
This repository contains a solution to process and analyze customer and order data from multiple sources (CSV and XML) using both database table and data-frame (in-memory) approaches. 

## To implement 
Two approaches to process the data:

1. **A. TABLE-BASED APPROACH** 
   - Data Cleaning & Loading  
   - Querying for KPIs  

2. **B. IN-MEMORY APPROACH** 
   - Data Cleaning  
   - KPI Functions  

Both approaches read customer data from a CSV file and order data from an XML file.

## KPIs performed:
1. **Repeat Customers** - customers with more than one order.  
2. **Monthly Order Trends** - Aggregate orders by month to observe trends.  
3. **Regional Revenue** - Sum of total_amount grouped by region.  
4. **Top Customers (Last 30 Days)** — Rank customers by total spend in the last 30 days.

---
## SETUP INSTRUCTIONS

1. Clone the Repository

    `git clone https://github.com/kusuma-hk/akasa_air_task.git` <br>
    `cd akasa_air_task `

2. Install dependencies
(Make sure you have Python 3.10+ installed for libraries version number compatibility)

    `pip install -r requirements.txt `


3. Prepared data (input files) are in the /data/ folder:
- data/task_DE_new_customers.csv
- data/task_DE_new_orders.xml

---

## TO RUN THE APPLICATION

**1. Table-Based approach**

Step 1: Create the Database

Open MySQL Workbench (or MySQL CLI) and create a new empty database: <br>
    `CREATE DATABASE akasa_air;`

Step 2: Set Up Environment Variables<br>
Copy .env.example → .env

Then fill in your MySQL credentials (i.e., only your password):<br>
`DB_TYPE=mysql` <br>
`DB_NAME=akasa_air` <br>
`DB_USER=root`   <br>
`DB_PASS=<your_password(e.g.,unicorn090)> ` <br>
`DB_HOST=localhost  ` <br>
`DB_PORT=3306   `

Step 3: Final step for first approach, run the ETL script in terminal: <br>
    `python src/etl_database.py`

After successfully executing, we should get respective CSV files inside the /outputs/ folder (for reference and to show results, output files are pushed into GitHub repo as well) which are:
- repeat_customers.csv
- monthly_order_trends.csv
- regional_revenue.csv
- top_spenders_30days.csv


Step 4: Verify in MySQL
To check the tables inside MySQL Workbench:
    
    `USE akasa_air;` <br>
    `SHOW TABLES;` <br>
    `SELECT COUNT(*) FROM orders;`


**2. In-Memory approach**
This approach performs all transformations in Python using Pandas, without a database.
It’s faster for smaller datasets and ideal for quick analytics or prototyping.

Step 1: Run the In-Memory Script:

    `python src/etl_inmemory.py`

Step 2: After successfully executing we should get respective CSV files inside /outputs/ folder which are:

- inmemory_repeat_customers.csv
- inmemory_monthly_trends.csv
- inmemory_regional_revenue.csv
- inmemory_top_spenders_30days.csv

---
