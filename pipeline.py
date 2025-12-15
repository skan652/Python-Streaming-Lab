"""
pipeline.py
----------------
This file launches BOTH producer and consumer using Python threads to simulate
a real streaming pipeline.

"""

import threading
from queue import Queue
from producer import CSVProducer
from consumer import Consumer

def main():
    q = Queue()

    producer = CSVProducer("transactions.csv", q, delay=1.0)
    consumer = Consumer(q)

    producer_thread = threading.Thread(target=producer.start)
    consumer_thread = threading.Thread(target=consumer.start)

    producer_thread.start()
    consumer_thread.start()

    producer_thread.join()
    consumer_thread.join()

if __name__ == "__main__":
    print("Starting the Python Streaming Pipeline...")
    main()
