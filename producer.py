"""
producer.py
----------------
This file contains the logic for a Producer that simulates a real-time data
stream by reading events from a CSV file and sending them one by one into a queue.

"""

import csv
import time
import random
from queue import Queue

class CSVProducer:
    def __init__(self, csv_path, q, delay=1.0):
        self.csv_path = csv_path
        self.q = q
        self.delay = delay
    def start(self):
        with open(self.csv_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                print(f"Sending row: {row}")
                self.q.put(row)
                time.sleep(self.delay + random.uniform(-0.1, 0.1))  # Add jitter


# Debugging test

if __name__ == "__main__":
    producer = CSVProducer("transactions.csv", Queue(), delay=1.0)
    producer.start()