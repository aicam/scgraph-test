from dataclasses import dataclass
import random
from typing import List

from lib.db import Operation

@dataclass
class TChopped:
    hops: List[List[Operation]]
    hops_table: List[str]
    TID: int = random.randint(0, 1000)


def chop_T(t: List[Operation]) -> TChopped:
    current_table = t[0].table.name
    current_hop = [t[0]]
    hops = []
    hops_table = [current_table]

    for operation in t[1:]:
        if operation.table.name != current_table:
            hops.append(current_hop)
            hops_table.append(current_table)
            current_hop = [operation]
            current_table = operation.table.name
        else:
            current_hop.append(operation)

    if current_hop:
        hops.append(current_hop)

    return TChopped(hops=hops, hops_table=hops_table)