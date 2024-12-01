import logging
import threading
import queue
from typing import List
from dataclasses import dataclass
from lib.db import Operation, Table


@dataclass
class Lock:
    table: str
    type: str  # "S" for shared, "X" for exclusive
    condition: dict


class SlaveProcessor:
    def __init__(self, table: Table, persistent: bool = False):
        self.table = table
        self.logger = logging.getLogger(self.__class__.__name__)
        self.hops_queue = queue.Queue()
        self.locks = []  # A list to maintain active locks
        self.lock = threading.Lock()  # Thread lock for synchronizing access to the locks list
        self.thread = threading.Thread(target=self._process_hops, daemon=True)
        self.stop_signal = threading.Event()
        self.thread.start()
        self.logger.info(f"Initialized SlaveProcessor for table: {self.table.name} with persistent equal to {persistent}")
        self.persistent = persistent

    def push_hop(self, hop: List[Operation]):
        """Push a list of hops (transactions) to the queue."""
        self.hops_queue.put(hop)

    def _process_hops(self):
        """Process hops in the queue."""
        while not self.stop_signal.is_set():
            try:
                hop = self.hops_queue.get(timeout=1)  # Fetch a hop
                self.logger.info(f"SlaveProcessor for table: {self.table.name} is processing a hop")
                if self._process_hop(hop):
                    self._release_locks(hop)  # Release locks if the hop is fully executed
                else:
                    self.hops_queue.put(hop)  # Re-queue the hop if not complete
            except queue.Empty:
                continue  # No hops, keep looping

    def _process_hop(self, hop: List[Operation]) -> bool:
        """Process a single hop (list of operations)."""
        all_operations_complete = True

        for operation in hop:
            if operation.executed:
                continue  # Skip already executed operations

            if self._can_acquire_lock(operation):
                self._acquire_lock(operation)
                operation.execute()
                if self.persistent:
                    self.table.save()
                self.logger.info(f"SlaveProcessor for table: {self.table.name} processed an operation {operation.operation_type}:{operation.condition}:{operation.new_value}")
            else:
                all_operations_complete = False  # Cannot process this operation yet

        return all_operations_complete

    def _can_acquire_lock(self, operation: Operation) -> bool:
        """Check if a lock can be acquired for the given operation."""
        with self.lock:
            for lock in self.locks:
                if (
                    lock.table == operation.table.name
                    and lock.condition["k"] == operation.condition["k"]
                    and lock.condition["v"] == operation.condition["v"]
                ):
                    # Conflict if an exclusive lock exists or if it's a write operation
                    if lock.type == "X" or operation.operation_type != "select":
                        return False
        return True

    def _acquire_lock(self, operation: Operation):
        """Acquire a lock for the given operation."""
        lock_type = "X" if operation.operation_type != "select" else "S"
        new_lock = Lock(table=operation.table.name, type=lock_type, condition=operation.condition)
        with self.lock:
            self.locks.append(new_lock)
        print(f"Acquired {lock_type} lock on {operation.condition} for table {operation.table.name}")

    def _release_locks(self, hop: List[Operation]):
        """Release all locks associated with a completed hop."""
        with self.lock:
            for operation in hop:
                self.locks = [
                    lock
                    for lock in self.locks
                    if not (
                        lock.table == operation.table.name
                        and lock.condition["k"] == operation.condition["k"]
                        and lock.condition["v"] == operation.condition["v"]
                    )
                ]
        print(f"Released locks for hop: {[op.operation_type for op in hop]}")

    def stop(self):
        """Stop the processing thread."""
        self.stop_signal.set()
        self.thread.join()
