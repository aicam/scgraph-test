import abc
import time
from typing import List, Union
import threading

from lib.chop import chop_T
from lib.log_config import setup_logging
from lib.masterprocessor import MasterProcessor
from lib.ops_generate import generate_T1, generate_T2, generate_T3, generate_T4, generate_T_err
from lib.db import Table, Operation
import pandas as pd
from lib.slaveprocessor import SlaveProcessor


# Function to generate transactions based on type
def generate_transactions(transaction_types: List[str], follow_table, user_table, tweet_table):
    generators = {
        "T1": lambda: generate_T1(follow_table, user_table),
        "T2": lambda: generate_T2(tweet_table, user_table),
        "T3": lambda: generate_T3(tweet_table, user_table),
        "T4": lambda: generate_T4(tweet_table, user_table),
        "T_err": lambda: generate_T_err(follow_table, user_table),
    }
    transactions = []
    for transaction_type in transaction_types:
        if transaction_type in generators:
            transactions.append(generators[transaction_type]())
    return transactions


# Thread function to push transactions
def push_transaction(master_processor, transaction):
    master_processor.lazy_push_T(transaction)


if __name__ == "__main__":
    setup_logging("db.log")

    # Initialize tables
    tweet_table = Table("tweet.csv")
    user_table = Table("user.csv")
    follow_table = Table("follow.csv")

    # Initialize processors
    tweet_table_processor = SlaveProcessor(tweet_table, "tweet")
    user_table_processor = SlaveProcessor(user_table, "user")
    follow_table_processor = SlaveProcessor(follow_table, "follow")

    master_processor = MasterProcessor([tweet_table_processor, user_table_processor, follow_table_processor])
    master_processor.start()

    # Configuration
    transaction_types = ["T1", "T2", "T3", "T4", "T_err"]  # Specify types of transactions to include
    num_transactions = 20  # Specify the number of transactions to send

    threads = []
    for _ in range(num_transactions):
        transactions = generate_transactions(transaction_types, follow_table, user_table, tweet_table)
        for transaction in transactions:
            thread = threading.Thread(target=push_transaction, args=(master_processor, transaction))
            threads.append(thread)
            thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Monitor and wait for the queue to empty
    while master_processor.transactions_queue or master_processor.hops_tracking:
        time.sleep(1)  # Check periodically if the processing is complete

    master_processor.stop()
