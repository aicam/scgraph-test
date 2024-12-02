import logging
import threading
import time
from typing import List, Dict, Tuple
from datetime import datetime

from lib.chop import TChopped, chop_T
from lib.db import Operation
from lib.slaveprocessor import SlaveProcessor


class MasterProcessor:
    def __init__(self, slaves: List[SlaveProcessor]):
        self.slaves = {slave.table.name: slave for slave in slaves}  # Map table names to SlaveProcessors
        self.transactions_queue: List[TChopped] = []  # Queue of chopped transactions
        self.hops_tracking: List[Tuple[List[Operation], str, float]] = []  # Track hops with start time
        self.logger = logging.getLogger(self.__class__.__name__)
        self.stop_signal = threading.Event()
        self.transaction_thread = threading.Thread(target=self._process_transactions, daemon=True)
        self.tracking_thread = threading.Thread(target=self._track_hops_completion, daemon=True)

    def push_T(self, T: List[Operation]):
        """Chop a transaction and add it to the queue."""
        T_chopped = chop_T(T)
        self.transactions_queue.append(T_chopped)
        self.logger.info(f"Transaction {T_chopped.TID} added to the queue with {len(T_chopped.hops)} hops.")

    def lazy_push_T(self, T: List[Operation]):
        """Chop a transaction and add it to the queue."""
        T_chopped = chop_T(T)
        delay = False
        for transaction in list(self.transactions_queue):  # Iterate over a copy to modify the original queue
            if len(T_chopped.hops) != len(transaction.hops):
                break
            for i in range(len(transaction.hops)):
                if len(T_chopped.hops[i]) != len(transaction.hops[i]):
                    break
                for j in range(len(transaction.hops[i])):
                    if T_chopped.hops[i][j].operation_type != transaction.hops[i][j].operation_type:
                        break
                    elif T_chopped.hops[i][j].condition != transaction.hops[i][j].condition:
                        break
                    else:
                        delay = True
                        break
        if delay:
            time.sleep(1)
            self.logger.warning(f"Transaction {T_chopped.TID} delayed.")
        self.transactions_queue.append(T_chopped)

    def start(self):
        """Start processing transactions."""
        self.transaction_thread.start()
        self.tracking_thread.start()
        self.logger.info("MasterProcessor started.")

    def stop(self):
        """Stop the processing threads."""
        self.stop_signal.set()
        self.transaction_thread.join()
        self.tracking_thread.join()
        self.logger.info("MasterProcessor stopped.")

    def _process_transactions(self):
        """Process transactions and delegate their hops to the corresponding SlaveProcessors."""
        while not self.stop_signal.is_set():
            for transaction in list(self.transactions_queue):  # Iterate over a copy to modify the original queue
                for i, hop in enumerate(transaction.hops):
                    table_name = transaction.hops_table[i]
                    slave = self.slaves.get(table_name)

                    if not slave:
                        self.logger.error(f"No SlaveProcessor found for table {table_name}. Skipping hop.")
                        continue

                    self.logger.info(
                        f"Pushing hop {i + 1}/{len(transaction.hops)} of transaction {transaction.TID} "
                        f"to SlaveProcessor for table {table_name}."
                    )
                    slave.push_hop(hop)

                    # Start tracking the hop
                    self.hops_tracking.append((hop, table_name, time.time()))

                # Remove the transaction after all its hops have been pushed
                self.transactions_queue.remove(transaction)
                self.logger.info(f"Transaction {transaction.TID} has been pushed to SlaveProcessors and removed from the queue.")

    def _track_hops_completion(self):
        """Track completion of hops and log the time taken."""
        while not self.stop_signal.is_set():
            time.sleep(0.3)  # Check every 300 milliseconds
            for hop, table_name, start_time in list(self.hops_tracking):  # Iterate over a copy to modify the original list
                if all(op.executed for op in hop):  # Check if all operations in the hop are executed
                    end_time = time.time()
                    duration = end_time - start_time
                    self.hops_tracking.remove((hop, table_name, start_time))
                    self.logger.info(
                        f"RECORD: Hop for table '{table_name}' completed in {duration:.3f} seconds."
                    )
                else:
                    end_time = time.time()
                    duration = end_time - start_time
                    if duration > 3:
                        self.logger.info(
                            f"RECORD: Hop for table '{table_name}' is taking longer than expected: {duration:.3f} seconds."
                        )