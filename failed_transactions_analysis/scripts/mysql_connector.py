import csv
import mysql.connector
from mysql.connector import Error
from datetime import datetime

# MySQL Configuration (replace with your actual Cloud SQL credentials)
config = {
    'host': '34.59.25.123',
    'user': 'analysis',
    'password': 'trans',
    'database': 'transactions',
    'port': 3306
}

# Path to filtered transactions CSV
csv_file_path = 'output/failed_txns.csv'

try:
    # Connect to MySQL
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()

    # Read CSV and insert data
    with open(csv_file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            query = """
                INSERT INTO failed_transactions 
                (transaction_id, amount, status, city, branch, date, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    amount=VALUES(amount),
                    status=VALUES(status),
                    city=VALUES(city),
                    branch=VALUES(branch),
                    date=VALUES(date),
                    timestamp=VALUES(timestamp);
            """
            data = (
                row['transaction_id'],
                float(row['amount']),
                row['status'],
                row['city'],
                row['branch'],
                row['date'],
                timestamp
            )
            cursor.execute(query, data)

    connection.commit()
    print("✅ Data successfully uploaded to MySQL.")

except Error as e:
    print(f"❌ Error: {e}")

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'connection' in locals() and connection.is_connected():
        connection.close()
