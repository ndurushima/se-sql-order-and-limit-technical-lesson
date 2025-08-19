import pandas as pd
import sqlite3
conn = sqlite3.connect("data.sqlite")


# products_table = pd.read_sql("""
#     SELECT * FROM products;
# """, conn)

# print(products_table)


order_by_name = pd.read_sql("""
    SELECT * FROM products
    ORDER BY productName;
""", conn)

print(order_by_name)


order_by_name_asc = pd.read_sql("""
    SELECT * FROM products
    ORDER BY productName ASC;
""", conn)

print(order_by_name_asc)


order_by_name_desc = pd.read_sql("""
    SELECT * FROM products
    ORDER BY productName DESC;
""", conn)

print(order_by_name_desc)





sort_str_length = pd.read_sql("""
    SELECT productName, length(productDescription) AS description_length FROM products
    ORDER BY description_length;
""", conn)

print(sort_str_length)


sort_str_length_2 = pd.read_sql("""
    SELECT productName FROM products
    ORDER BY length(productDescription);
""", conn)

print(sort_str_length_2)


sort_multiple_columns = pd.read_sql("""
    SELECT productVendor, productName, MSRP FROM products
    ORDER BY productVendor, productName;
""", conn)

print(sort_multiple_columns)


sort_multiple_columns_weird = pd.read_sql("""
    SELECT productVendor, productName, MSRP FROM products
    ORDER BY productName, productVendor;
""", conn)

print(sort_multiple_columns_weird)


unique_values = pd.read_sql("""
    SELECT COUNT(DISTINCT productVendor) AS num_product_vendors, COUNT(DISTINCT productName) AS num_product_names FROM products;
""", conn)

print(unique_values)


sort_in_stock = pd.read_sql("""
    SELECT productName, quantityInStock
    FROM products
    ORDER BY quantityInStock;
""", conn)

print(sort_in_stock)

# Issue for both above and below is that we are taking the qunatity in Stock as a str and not an integer

sort_in_stock_10 = pd.read_sql("""
    SELECT productName, quantityInStock
    FROM products
    ORDER BY quantityInStock;
""", conn).head(10)

print(sort_in_stock_10)


sort_in_stock_int = pd.read_sql("""
    SELECT productName, quantityInStock
    FROM products
    ORDER BY CAST(quantityInStock AS INTEGER);
""", conn).head(10)

print(sort_in_stock_int)


sort_in_stock_int_all = pd.read_sql("""
SELECT productName, quantityInStock
  FROM products
 ORDER BY CAST(quantityInStock AS INTEGER);
""", conn)

print(sort_in_stock_int_all)





limit_order_5 = pd.read_sql("""
SELECT *
  FROM orders
 LIMIT 5;
""", conn)

print(limit_order_5)


longest_comments = pd.read_sql("""
SELECT *
  FROM orders
 ORDER BY length(comments) DESC
 LIMIT 10;
""", conn)

print(longest_comments)


longest_comments_cancelled = pd.read_sql("""
SELECT *
  FROM orders
 WHERE status = "Cancelled"
 ORDER BY length(comments) DESC
 LIMIT 10;
""", conn)

print(longest_comments_cancelled)


longest_cancelled_resolved = pd.read_sql("""
SELECT *
  FROM orders
 WHERE status IN ("Cancelled", "Resolved")
 ORDER BY length(comments) DESC
 LIMIT 10;
""", conn)

print(longest_cancelled_resolved)


newest_order_date = pd.read_sql("""
SELECT *
  FROM orders
 WHERE shippedDate = ""
   AND status != "Cancelled"
 ORDER BY orderDate DESC
 LIMIT 10;
""", conn)

print(newest_order_date)


longest_to_fulfill = pd.read_sql("""
SELECT *,
       julianday(shippedDate) - julianday(orderDate) AS days_to_fulfill
  FROM orders
 WHERE shippedDate != ""
 ORDER BY days_to_fulfill DESC
 LIMIT 1;
""", conn)

print(longest_to_fulfill)