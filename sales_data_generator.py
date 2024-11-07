# data_generator.py
import json
import random
from datetime import datetime
import time

def generate_sales_data():
    return {
        'AgentID': f'AGT{random.randint(100, 999)}',
        'OfficeID': f'OFF{random.randint(10, 99)}',
        'CarID': f'CAR{random.randint(100, 999)}',
        'CustomerID': f'CUST{random.randint(1000, 9999)}',
        'Amount': round(random.uniform(10.0, 500.0), 2), 
        'TrxnTimestamp': datetime.utcnow().isoformat()
    }

def generate_sales_data_batch(batch_size=5):
    return [generate_sales_data() for _ in range(batch_size)]

if __name__ == "__main__":
    while True:
        batch = generate_sales_data_batch()
        print("Generated sales data batch:", json.dumps(batch, indent=2))
        time.sleep(1)
