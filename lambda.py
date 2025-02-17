import json
import base64
import mysql.connector
import os

# Database connection function
def get_db_connection():
    conn = mysql.connector.connect(
        host="xxxxxxx",
        user="xxxxxx",
        password="xxxxxxx",
        database="database1"
    )
    return conn

# Lambda function to handle incoming Kafka message
def lambda_handler(event, context):
    conn = None
    cursor = None

    try:
      messages=[
      {
        "order_id": "ORD234567",
        "customer": {
          "customer_id": "CUST001",
          "first_name": "Amit",
          "last_name": "Sharma",
          "email": "amitsharma@example.com",
          "phone": "+918765432100",
          "shipping_address": {
            "street": "45 Park Avenue",
            "city": "Mumbai",
            "state": "MH",
            "postal_code": "400001",
            "country": "India"
          }
        },
        "order_details": {
          "items": [
            {
              "product_id": "PROD789",
              "product_name": "LED Monitor",
              "quantity": 1,
              "price_per_unit": 7999.99
            },
            {
              "product_id": "PROD012",
              "product_name": "USB-C Hub",
              "quantity": 2,
              "price_per_unit": 1499.99
            }
          ],
          "subtotal": 10999.97,
          "tax": 1979.99,
          "shipping_fee": 200.00,
          "total_amount": 13279.96
        },
        "payment": {
          "payment_method": "UPI",
          "payment_status": "Completed",
          "transaction_id": "TXN1234567890",
          "payment_date": "2024-12-13T11:00:00Z"
        },
        "order_status": "Shipped",
        "timestamp": "2024-12-13T11:05:00Z"
      },
      {
        "order_id": "ORD345678",
        "customer": {
          "customer_id": "CUST002",
          "first_name": "Priya",
          "last_name": "Mehta",
          "email": "priyamehta@example.com",
          "phone": "+919876543210",
          "shipping_address": {
            "street": "12 MG Road",
            "city": "Delhi",
            "state": "DL",
            "postal_code": "110001",
            "country": "India"
          }
        },
        "order_details": {
          "items": [
            {
              "product_id": "PROD345",
              "product_name": "Power Bank",
              "quantity": 1,
              "price_per_unit": 1499.00
            },
            {
              "product_id": "PROD678",
              "product_name": "Smartphone Case",
              "quantity": 3,
              "price_per_unit": 299.00
            }
          ],
          "subtotal": 2396.00,
          "tax": 431.28,
          "shipping_fee": 100.00,
          "total_amount": 2927.28
        },
        "payment": {
          "payment_method": "Debit Card",
          "payment_status": "Completed",
          "transaction_id": "TXN2345678901",
          "payment_date": "2024-12-13T12:00:00Z"
        },
        "order_status": "Delivered",
        "timestamp": "2024-12-13T12:05:00Z"
      },
      {
        "order_id": "ORD456789",
        "customer": {
          "customer_id": "CUST003",
          "first_name": "Rahul",
          "last_name": "Kumar",
          "email": "rahulkumar@example.com",
          "phone": "+917654321098",
          "shipping_address": {
            "street": "88 Nehru Street",
            "city": "Chennai",
            "state": "TN",
            "postal_code": "600001",
            "country": "India"
          }
        },
        "order_details": {
          "items": [
            {
              "product_id": "PROD901",
              "product_name": "Gaming Mouse",
              "quantity": 1,
              "price_per_unit": 2499.00
            },
            {
              "product_id": "PROD234",
              "product_name": "Gaming Keyboard",
              "quantity": 1,
              "price_per_unit": 3999.00
            }
          ],
          "subtotal": 6498.00,
          "tax": 1169.64,
          "shipping_fee": 150.00,
          "total_amount": 7817.64
        },
        "payment": {
          "payment_method": "Net Banking",
          "payment_status": "Completed",
          "transaction_id": "TXN3456789012",
          "payment_date": "2024-12-13T13:00:00Z"
        },
        "order_status": "Out for Delivery",
        "timestamp": "2024-12-13T13:05:00Z"
      },
      {
        "order_id": "ORD567890",
        "customer": {
          "customer_id": "CUST004",
          "first_name": "Anjali",
          "last_name": "Verma",
          "email": "anjaliverma@example.com",
          "phone": "+919012345678",
          "shipping_address": {
            "street": "22 Sunrise Lane",
            "city": "Pune",
            "state": "MH",
            "postal_code": "411001",
            "country": "India"
          }
        },
        "order_details": {
          "items": [
            {
              "product_id": "PROD567",
              "product_name": "Wireless Earbuds",
              "quantity": 1,
              "price_per_unit": 2999.00
            }
          ],
          "subtotal": 2999.00,
          "tax": 539.82,
          "shipping_fee": 100.00,
          "total_amount": 3638.82
        },
        "payment": {
          "payment_method": "Credit Card",
          "payment_status": "Completed",
          "transaction_id": "TXN4567890123",
          "payment_date": "2024-12-13T14:00:00Z"
        },
        "order_status": "Processing",
        "timestamp": "2024-12-13T14:05:00Z"
      },
      {
        "order_id": "ORD678901",
        "customer": {
          "customer_id": "CUST005",
          "first_name": "Deepak",
          "last_name": "Singh",
          "email": "deepaksingh@example.com",
          "phone": "+918123456789",
          "shipping_address": {
            "street": "33 Lake Road",
            "city": "Kolkata",
            "state": "WB",
            "postal_code": "700001",
            "country": "India"
          }
        },
        "order_details": {
          "items": [
            {
              "product_id": "PROD678",
              "product_name": "Smart Watch",
              "quantity": 1,
              "price_per_unit": 5999.00
            },
            {
              "product_id": "PROD789",
              "product_name": "Fitness Band",
              "quantity": 1,
              "price_per_unit": 1999.00
            }
          ],
          "subtotal": 7998.00,
          "tax": 1439.64,
          "shipping_fee": 200.00,
          "total_amount": 9637.64
        },
        "payment": {
          "payment_method": "UPI",
          "payment_status": "Completed",
          "transaction_id": "TXN5678901234",
          "payment_date": "2024-12-13T15:00:00Z"
        },
        "order_status": "Delivered",
        "timestamp": "2024-12-13T15:05:00Z"
      },
      {
        "order_id": "ORD789012",
        "customer": {
          "customer_id": "CUST006",
          "first_name": "Neha",
          "last_name": "Kapoor",
          "email": "nehakapoor@example.com",
          "phone": "+917891234567",
          "shipping_address": {
            "street": "10 Palace Road",
            "city": "Bangalore",
            "state": "KA",
            "postal_code": "560001",
            "country": "India"
          }
        },
        "order_details": {
          "items": [
            {
              "product_id": "PROD890",
              "product_name": "Portable Speaker",
              "quantity": 2,
              "price_per_unit": 2499.00
            }
          ],
          "subtotal": 4998.00,
          "tax": 899.64,
          "shipping_fee": 150.00,
          "total_amount": 6047.64
        },
        "payment": {
          "payment_method": "Debit Card",
          "payment_status": "Completed",
          "transaction_id": "TXN6789012345",
          "payment_date": "2024-12-13T16:00:00Z"
        },
        "order_status": "Shipped",
        "timestamp": "2024-12-13T16:05:00Z"
      }
      ]



        for message in messages:
            #message = event['records']["Test_Topic-5"][0]['value']
            #print(message)
        
            customer_data = message['customer']
            order_data = message['order_details']
            payment_data = message['payment']
            
            
            conn = get_db_connection()
            cursor = conn.cursor()

            # Start transaction
            conn.start_transaction()

            # Check if customer exists, and insert if necessary
            cursor.execute("SELECT * FROM customers WHERE customer_id = %s", (customer_data['customer_id'],))
            
            if cursor.fetchone() is None:
                customer_query = """
                    INSERT INTO customers (customer_id, first_name, last_name, email, phone, street, city, state, postal_code, country)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                customer_values = (
                    customer_data['customer_id'], customer_data['first_name'], customer_data['last_name'], customer_data['email'], 
                    customer_data['phone'], customer_data['shipping_address']['street'], customer_data['shipping_address']['city'], 
                    customer_data['shipping_address']['state'], customer_data['shipping_address']['postal_code'], 
                    customer_data['shipping_address']['country']
                )
                cursor.execute(customer_query, customer_values)

            # Insert or update order
            cursor.execute("SELECT * FROM orders WHERE order_id = %s", (message['order_id'],))
            existing_order = cursor.fetchone()

            if existing_order:
                update_order_query = """
                    UPDATE orders
                    SET order_status = %s, subtotal = %s, tax = %s, shipping_fee = %s, total_amount = %s, payment_status = %s, timestamp = %s
                    WHERE order_id = %s;
                """
                update_order_values = (
                    message['order_status'], order_data['subtotal'], order_data['tax'], order_data['shipping_fee'], 
                    order_data['total_amount'], payment_data['payment_status'], message['timestamp'], message['order_id']
                )
                cursor.execute(update_order_query, update_order_values)
            else:
                insert_order_query = """
                    INSERT INTO orders (order_id, customer_id, order_status, subtotal, tax, shipping_fee, total_amount, payment_status, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                insert_order_values = (
                    message['order_id'], customer_data['customer_id'], message['order_status'], 
                    order_data['subtotal'], order_data['tax'], order_data['shipping_fee'], 
                    order_data['total_amount'], payment_data['payment_status'], message['timestamp']
                )
                cursor.execute(insert_order_query, insert_order_values)
            
            # Insert order details
            for item in order_data['items']:
                order_detail_query = """
                    INSERT INTO order_details (order_id, product_id, product_name, quantity, price_per_unit)
                    VALUES (%s, %s, %s, %s, %s);
                """
                order_detail_values = (message['order_id'], item['product_id'], item['product_name'], item['quantity'], item['price_per_unit'])
                cursor.execute(order_detail_query, order_detail_values)
            
            # Insert payment data
            payment_query = """
                INSERT INTO payments (order_id, payment_method, payment_status, transaction_id, payment_date)
                VALUES (%s, %s, %s, %s, %s);
            """
            payment_values = (
                message['order_id'], payment_data['payment_method'], payment_data['payment_status'], 
                payment_data['transaction_id'], payment_data['payment_date']
            )
            cursor.execute(payment_query, payment_values)
            
            # Commit transaction
            conn.commit()
        
    except Exception as e:
        print(f"Error processing message: {str(e)}")
        conn.rollback()  # Rollback if any exception occurs
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Data processed successfully!')
    }
