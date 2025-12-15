"""
consumer.py
----------------
This file contains the logic for a Consumer that continuously reads messages
from the queue and processes them.

The Consumer runs forever, just like a streaming system.
"""

from collections import defaultdict
import time
from queue import Queue

class Consumer:
    def __init__(self, q):
        self.q = q
        self.consumed_count = 0
        self.running_total = 0
        self.total_by_type = defaultdict(float)


    def start(self):
        for _event in iter(self.q.get, None):
            print(f"Consumed event: {_event}")
            self.process(_event)
            self.consumed_count += 1
            print(f"Total events consumed: {self.consumed_count}")
    

    def process(self, event):
        time.sleep(0.5)  # Simulate processing time
        amount = event.get("amount", 0)
        event_type = event.get("type", "unknown")
        self.running_total += amount
        self.consumed_count += 1
        self.total_by_type[event_type] += amount
        print(f"Running total: {self.running_total}",
              "amount added:", amount,
              "consumed count:", {self.consumed_count},
              "totals by type:", dict(self.total_by_type))
        

# Debugging test

if __name__ == "__main__":
    q = Queue()
    consumer = Consumer(q)
    consumer.start()
