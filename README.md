<<<<<<< Updated upstream
# Dummy Data Generator

## Overview

This project generates dummy data for specified tables in a MySQL database. 

## Setup

1. Install dependencies:
   ```bash
   pip install sqlalchemy faker pyyaml pymysql
=======
# MySQL to MongoDB Migration

## Table Comparisons

## orders
   - MySQL:
     - Columns: order_id (int), customer_id (int), order_date (date), order_status (varchar(50)), total_amount (decimal(10,2)), payment_method (varchar(50)), shipping_address (varchar(255)), delivery_date (date)
   - MongoDB:
     - Fields: order_id (NumberInt), customer_id (NumberInt), order_date (String), order_status (String), total_amount (String), payment_method (String), shipping_address (String), delivery_date (String)

## reviews
   - MySQL:
     - Columns: review_id (int), product_id (int), customer_id (int), rating (int), review_text (text), review_date (timestamp), status (varchar(50)), response (text)
   - MongoDB:
     - Fields: review_id (NumberInt), product_id (NumberInt), customer_id (NumberInt), rating (NumberInt), review_text (String), review_date (String), status (String), response (String)


## Migration Notes

- All integer IDs from MySQL are preserved in MongoDB, but MongoDB also adds its own `_id` field.
- MySQL's `datetime` type is converted to MongoDB's `date` type.
- Decimal fields in MySQL are converted to double in MongoDB.
>>>>>>> Stashed changes
