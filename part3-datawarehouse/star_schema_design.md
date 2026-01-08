# Star Schema Design – FlexiMart Data Warehouse

## Section 1: Schema Overview

The FlexiMart data warehouse is designed using a star schema to support
analytical reporting and business intelligence queries related to sales
performance.

### FACT TABLE: fact_sales

Grain:
One row represents one product sold in a single order line item.

Business Process:
Sales transactions recorded at the time of purchase.

Measures (Numeric Facts):
- quantity_sold: Number of units sold in a transaction
- unit_price: Price per unit at the time of sale
- discount_amount: Discount applied to the transaction
- total_amount: Final sales amount calculated as
  (quantity_sold × unit_price – discount_amount)

Foreign Keys:
- date_key → dim_date
- product_key → dim_product
- customer_key → dim_customer

---

### DIMENSION TABLE: dim_date

Purpose:
Stores date-related attributes to enable time-based analysis.

Type:
Conformed dimension shared across analytical queries.

Attributes:
- date_key (PK): Surrogate key in YYYYMMDD format
- full_date: Actual calendar date
- day_of_week: Name of the day (Monday, Tuesday, etc.)
- day_of_month: Numeric day of the month
- month: Month number (1–12)
- month_name: Month name (January, February, etc.)
- quarter: Calendar quarter (Q1, Q2, Q3, Q4)
- year: Calendar year
- is_weekend: Boolean flag indicating weekend or weekday

---

### DIMENSION TABLE: dim_product

Purpose:
Stores descriptive information about products.

Attributes:
- product_key (PK): Surrogate key
- product_id: Business product identifier
- product_name: Name of the product
- category: High-level product category
- subcategory: Detailed product classification
- unit_price: Standard product price

---

### DIMENSION TABLE: dim_customer

Purpose:
Stores descriptive information about customers.

Attributes:
- customer_key (PK): Surrogate key
- customer_id: Business customer identifier
- customer_name: Full name of the customer
- city: Customer city
- state: Customer state
- customer_segment: Customer classification (e.g., Retail, Corporate)

## Section 2: Design Decisions

The granularity of the fact table is defined at the transaction line-item
level because it provides the highest level of detail for analysis. This
allows the business to analyze sales by product, customer, date, and quantity
at a very detailed level, while still enabling aggregation for higher-level
reporting such as monthly or yearly sales.

Surrogate keys are used instead of natural keys to improve performance and
maintain consistency. Natural keys from source systems may change or contain
business meaning, whereas surrogate keys are stable, integer-based, and
efficient for joins in a data warehouse environment.

This star schema design supports drill-down and roll-up operations by allowing
queries to aggregate data across dimensions. Users can drill down from yearly
sales to monthly, daily, or product-level sales, or roll up detailed
transactions into summarized reports. The separation of facts and dimensions
simplifies queries and improves analytical performance.
## Section 3: Sample Data Flow

### Source Transaction
Order ID: 101  
Customer: John Doe  
Product: Laptop  
Quantity: 2  
Unit Price: ₹50,000  
Order Date: 15 January 2024  

### Data Warehouse Representation

fact_sales:
{
  date_key: 20240115,
  product_key: 5,
  customer_key: 12,
  quantity_sold: 2,
  unit_price: 50000,
  discount_amount: 0,
  total_amount: 100000
}

dim_date:
{
  date_key: 20240115,
  full_date: '2024-01-15',
  day_of_week: 'Monday',
  month: 1,
  quarter: 'Q1',
  year: 2024,
  is_weekend: false
}

dim_product:
{
  product_key: 5,
  product_name: 'Laptop',
  category: 'Electronics',
  subcategory: 'Computers'
}

dim_customer:
{
  customer_key: 12,
  customer_name: 'John Doe',
  city: 'Mumbai',
  state: 'Maharashtra',
  customer_segment: 'Retail'
}
