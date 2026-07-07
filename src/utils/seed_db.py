import psycopg2
from datetime import datetime

conn =psycopg2.connect(
    host = "localhost",
    database = "ecom_source",
    user = "data_engineer",
    password = "password123",
    port = "5432"
)

cursor = conn.cursor()

cursor.execute("""
               CREATE TABLE IF NOT EXISTS orders ( order_id SERIAL PRIMARY KEY,
               customer_id INT,
               amount DECIMAL(10, 2),
               status VARCHAR(50),
               created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
               updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
               );""")
initial_orders = [
    (101,250.50,'completed',datetime(2026, 7, 1, 10, 0)),
    (102, 12.99, 'pending', datetime(2026, 7, 1, 11,30)),
    (103, 89.00, 'completed',datetime(2026, 7, 2, 0, 15)),
]
for item in initial_orders: 
    cursor.execute("""
                   INSERT INTO orders (customer_id, amount, status, updated_at)
                   VALUES (%s, %s, %s,%s)""",item) 
    conn.commit()
print("Source database initialzed and seeded successfully!")
cursor.close()
conn.close()