import csv
import os
import random
from datetime import datetime, timedelta

# Updated cities
cities = ["Hyderabad", "Bangalore", "Chennai"]
branches_per_city = 5
transactions_per_file = 50
statuses = ["SUCCESS", "FAILED", "PENDING"]
output_dir = "../data"

# Ensure data folder exists
os.makedirs(output_dir, exist_ok=True)

# Generate data
for city in cities:
    for branch_num in range(1, branches_per_city + 1):
        branch_name = f"{city}_Branch{branch_num}"
        filename = f"{branch_name}.csv"
        filepath = os.path.join(output_dir, filename)

        with open(filepath, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["transaction_id", "amount", "status", "city", "branch", "date", "timestamp"])

            for i in range(transactions_per_file):
                txn_id = f"{branch_name}_{i+1:04}"
                amount = round(random.uniform(100.0, 5000.0), 2)
                status = random.choice(statuses)
                days_ago = random.randint(0, 6)
                txn_date = (datetime.now() - timedelta(days=days_ago)).date()
                txn_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                writer.writerow([txn_id, amount, status, city, branch_name, txn_date, txn_timestamp])

print("✅ Sample transaction files generated in /data folder.")
