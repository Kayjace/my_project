use oz;

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone_number VARCHAR(20),
    address VARCHAR(255),
    date_of_birth DATE,
    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    order_status VARCHAR(50),
    total_amount DECIMAL(10, 2),
    payment_method VARCHAR(50),
    shipping_address VARCHAR(255),
    delivery_date DATE
);

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2),
    stock_quantity INT,
    supplier_id INT,
    description TEXT,
    added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE suppliers (
    supplier_id INT PRIMARY KEY,
    supplier_name VARCHAR(100),
    contact_name VARCHAR(50),
    contact_email VARCHAR(100),
    phone_number VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(50),
    country VARCHAR(50)
);

CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone_number VARCHAR(20),
    hire_date DATE,
    department VARCHAR(50),
    salary DECIMAL(10, 2)
);

CREATE TABLE departments (
    department_id INT PRIMARY KEY,
    department_name VARCHAR(50),
    manager_id INT,
    location VARCHAR(50),
    budget DECIMAL(15, 2),
    start_date DATE,
    end_date DATE
);

CREATE TABLE inventory (
    inventory_id INT PRIMARY KEY,
    product_id INT,
    quantity INT,
    location VARCHAR(50),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reorder_level INT,
    reorder_quantity INT,
    supplier_id INT
);

CREATE TABLE invoices (
    invoice_id INT PRIMARY KEY,
    order_id INT,
    invoice_date DATE,
    due_date DATE,
    total_amount DECIMAL(10, 2),
    tax_amount DECIMAL(10, 2),
    status VARCHAR(50),
    payment_date DATE
);

CREATE TABLE shipments (
    shipment_id INT PRIMARY KEY,
    order_id INT,
    shipment_date DATE,
    carrier VARCHAR(50),
    tracking_number VARCHAR(100),
    status VARCHAR(50),
    estimated_arrival DATE,
    actual_arrival DATE
);

CREATE TABLE reviews (
    review_id INT PRIMARY KEY,
    product_id INT,
    customer_id INT,
    rating INT,
    review_text TEXT,
    review_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50),
    response TEXT
);