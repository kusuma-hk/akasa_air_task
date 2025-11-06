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

Both approaches read customer data from a CSV file and order data from an XML file

## KPIs performed:
1. **Repeat Customers** - customers with more than one order.  
2. **Monthly Order Trends** -  Aggregate orders by month to observe trends.  
3. **Regional Revenue** - Sum of total_amount grouped by region.  
4. **Top Customers (Last 30 Days)** — Rank customers by total spend in the last 30 days.
---

## Folder structure 

akasa_air_task/
│
├─ src/
│   ├─ etl_database.py          # Approach A: Table-based ETL
│   ├─ etl_inmemory.py          # Approach B: In-memory ETL
│   ├─ db_connection.py         # Secure DB connector 
├─ data/
│   ├─ task_DE_new_customers.csv
│   └─ task_DE_new_orders.xml
├─ outputs/                     # generated KPI reports or outputs in csv files
├─ .env.example                 # Environment variable template
├─ requirements.txt             # Required dependencies
├─ README.md                    # Project documentation
├─ .gitignore
└─ .gitattributes

---
## SETUP INSTRUCTIONS

1. Clone the Repository
git clone https://github.com/kusuma-hk/akasa_air_task.git
cd akasa_air_task

2. Install dependencies (Make sure you have Python 3.10+ installed for libraries version number compatibality)
pip install -r requirements.txt

3. Prepared data(input files) are in the /data/ folder:
data\task_DE_new_customers.csv
data\task_DE_new_orders.xml

## TO RUN THE APPLICATION

**1. Table- Based approach**
Step 1: Create the Database

Open MySQL Workbench (or MySQL CLI) and create a new empty database:
CREATE DATABASE akasa_air;

Step 2: Set Up Environment Variables

Copy .env.example -> .env
Then fill in your MySQL credentials(i.e.,only your password):
DB_TYPE=mysql
DB_NAME=akasa_air
DB_USER=root
DB_PASS=your_password
DB_HOST=localhost
DB_PORT=3306

Step 3: Final step for first approach, run the etl script in terminal:
python src/etl_database.py

After successfully executing we should get a respective csv files inside /outputs/ folder (for reference and to show results, output files are pushed into github repo as well).

**2. In-Memroy approach**
This approach performs all transformations in Python using Pandas, without a database.
It’s faster for smaller datasets and ideal for quick analytics or prototyping. 
Therefore,

Step 1: Run the In-Memory Script:

python src/etl_inmemory.py

Step 2: 