import pandas as pd
import mysql.connector
from dateutil import parser
import re

# ---------------- DATABASE CONNECTION ----------------
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="0497",   
    database="fleximart"
)

cursor = conn.cursor()

# ---------------- HELPER FUNCTIONS ----------------
def standardize_phone(phone):
    if pd.isna(phone):
        return None
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) == 10:
        return "+91-" + digits
    if len(digits) == 12 and digits.startswith("91"):
        return "+91-" + digits[2:]
    return None

def parse_date_safe(value):
    try:
        return parser.parse(str(value)).date()
    except:
        return None

# ---------------- DATA QUALITY REPORT ----------------
report = []

# ===================== CUSTOMERS =====================
customers = pd.read_csv("data/customers_raw.csv", header=None)

# Split the single column into real columns
customers = customers[0].str.split(",", expand=True)

# Set correct column names
customers.columns = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "city",
    "registration_date"
]
# Replace empty email strings with unique generated emails
customers["email"] = customers["email"].replace("", pd.NA)

customers["email"] = customers.apply(
    lambda row: f"unknown_{row['customer_id']}@example.com"
    if pd.isna(row["email"]) else row["email"],
    axis=1
)

print("CUSTOMER COLUMNS:", list(customers.columns))



original_customers = len(customers)

customers.drop_duplicates(subset=["customer_id"], inplace=True)
missing_emails = customers["email"].isna().sum()

customers["email"].fillna(
    "unknown_" + customers["customer_id"] + "@example.com",
    inplace=True
)

customers["phone"] = customers["phone"].apply(standardize_phone)
customers["registration_date"] = customers["registration_date"].apply(parse_date_safe)

customer_id_map = {}

for _, row in customers.iterrows():
    cursor.execute("""
        INSERT INTO customers
        (first_name, last_name, email, phone, city, registration_date)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        row.first_name,
        row.last_name,
        row.email,
        row.phone,
        row.city,
        row.registration_date
    ))
    customer_id_map[row.customer_id] = cursor.lastrowid

conn.commit()

report.append(
    f"Customers: processed={original_customers}, "
    f"duplicates_removed={original_customers - len(customers)}, "
    f"missing_emails_filled={missing_emails}, "
    f"loaded={len(customers)}"
)

# ===================== PRODUCTS =====================
products = pd.read_csv("data/products_raw.csv", header=None)
products = products[0].str.split(",", expand=True)

products.columns = [
    "product_id",
    "product_name",
    "category",
    "price",
    "stock_quantity"
]

original_products = len(products)

# Convert price and stock to numeric
products["price"] = pd.to_numeric(products["price"], errors="coerce")
products["stock_quantity"] = pd.to_numeric(products["stock_quantity"], errors="coerce")

missing_prices = products["price"].isna().sum()

# Fill missing numeric values
products["price"].fillna(products["price"].median(), inplace=True)
products["stock_quantity"].fillna(0, inplace=True)


product_id_map = {}

for _, row in products.iterrows():
    cursor.execute("""
        INSERT INTO products
        (product_name, category, price, stock_quantity)
        VALUES (%s, %s, %s, %s)
    """, (
        row.product_name.strip(),
        row.category,
        row.price,
        int(row.stock_quantity)
    ))
    product_id_map[row.product_id] = cursor.lastrowid

conn.commit()

report.append(
    f"Products: processed={original_products}, "
    f"missing_prices_filled={missing_prices}, "
    f"loaded={len(products)}"
)

# ===================== SALES =====================
sales = pd.read_csv("data/sales_raw.csv", header=None)
sales = sales[0].str.split(",", expand=True)

sales.columns = [
    "transaction_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "transaction_date",
    "status"
]
# Convert numeric sales columns
sales["quantity"] = pd.to_numeric(sales["quantity"], errors="coerce")
sales["unit_price"] = pd.to_numeric(sales["unit_price"], errors="coerce")
# Treat empty strings as missing
sales.replace("", pd.NA, inplace=True)

# Drop invalid sales rows (no customer or product)
sales.dropna(
    subset=["customer_id", "product_id", "quantity", "unit_price"],
    inplace=True
)

# Drop rows where quantity or price is missing
sales.dropna(subset=["quantity", "unit_price"], inplace=True)

original_sales = len(sales)

sales.drop_duplicates(subset=["transaction_id"], inplace=True)
sales.dropna(subset=["customer_id", "product_id"], inplace=True)
sales["transaction_date"] = sales["transaction_date"].apply(parse_date_safe)

loaded_sales = 0

for _, row in sales.iterrows():
    cust_id = customer_id_map[row.customer_id]
    prod_id = product_id_map[row.product_id]

    cursor.execute("""
        INSERT INTO orders
        (customer_id, order_date, total_amount, status)
        VALUES (%s, %s, %s, %s)
    """, (
        cust_id,
        row.transaction_date,
        row.quantity * row.unit_price,
        row.status
    ))

    order_id = cursor.lastrowid

    cursor.execute("""
        INSERT INTO order_items
        (order_id, product_id, quantity, unit_price, subtotal)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        order_id,
        prod_id,
        row.quantity,
        row.unit_price,
        row.quantity * row.unit_price
    ))

    loaded_sales += 1

conn.commit()

report.append(
    f"Sales: processed={original_sales}, "
    f"after_cleaning={len(sales)}, "
    f"loaded={loaded_sales}"
)

# ---------------- WRITE REPORT ----------------
with open("data_quality_report.txt", "w") as f:
    f.write("DATA QUALITY REPORT – FLEXIMART ETL\n")
    f.write("----------------------------------\n\n")

    f.write("CUSTOMERS FILE\n")
    f.write(f"Records processed           : {original_customers}\n")
    f.write(f"Duplicates removed           : {original_customers - len(customers)}\n")
    f.write(f"Missing values handled       : {missing_emails} (emails)\n")
    f.write(f"Records loaded successfully  : {len(customers)}\n\n")

    f.write("PRODUCTS FILE\n")
    f.write(f"Records processed           : {original_products}\n")
    f.write(f"Duplicates removed           : 0\n")
    f.write(f"Missing values handled       : {missing_prices} (prices)\n")
    f.write(f"Records loaded successfully  : {len(products)}\n\n")

    f.write("SALES FILE\n")
    f.write(f"Records processed           : {original_sales}\n")
    f.write(f"Duplicates removed           : {original_sales - len(sales)}\n")
    f.write("Missing values handled       : rows with missing customer/product/price\n")
    f.write(f"Records loaded successfully  : {loaded_sales}\n")


print("ETL PIPELINE COMPLETED SUCCESSFULLY")
