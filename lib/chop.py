from dataclasses import dataclass
from typing import List

from lib.db import Operation


class TChopped:
    hops = List[List[Operation]]
    finished_hop: int = -1

    def __init__(self, hops: List[List[Operation]]):
        self.hops = hops

    def forward(self):
        current_hop = self.hops[self.finished_hop + 1]
        for operation in current_hop:
            operation.execute()
        self.finished_hop += 1


def chop_T(t: List[Operation]):
    current_table = t[0].table.name
    current_hop = [t[0]]
    hops = []

    for operation in t[1:]:
        if operation.table.name != current_table:
            hops.append(current_hop)
            current_hop = [operation]
            current_table = operation.table.name
        else:
            current_hop.append(operation)

    if current_hop:
        hops.append(current_hop)

    return TChopped(hops=hops)