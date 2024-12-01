import abc
import time
from typing import List, Union

from lib.chop import chop_T
from lib.log_config import setup_logging
from lib.ops_generate import generate_T1, generate_T2, generate_T3, generate_T4
from lib.db import Table, Operation

import pandas as pd

from lib.slaveprocessor import SlaveProcessor

if __name__ == "__main__":
    setup_logging("db.log")

    tweet_table = Table("tweet.csv")
    user_table = Table("user.csv")
    follow_table = Table("follow.csv")

    tweet_table_processor = SlaveProcessor(tweet_table)
    user_table_processor = SlaveProcessor(user_table)
    follow_table_processor = SlaveProcessor(follow_table)

    # T1 = generate_T1(follow_table, user_table)
    # for op in T1:
    #     op.execute()
    # follow_table.save()
    # user_table.save()
    #

    # T2 = generate_T2(tweet_table, user_table)
    # for op in T2:
    #     op.execute()
    # tweet_table.save()
    # user_table.save()

    # T3 = generate_T3(tweet_table, user_table)
    # for op in T3:
    #     op.execute()
    # tweet_table.save()
    # user_table.save()
    #
    # T4 = generate_T4(tweet_table, user_table)
    # for op in T4:
    #     op.execute()
    # tweet_table.save()
    # user_table.save()

    T1 = generate_T1(follow_table, user_table)
    T1_chopped = chop_T(T1)
    for (i, hop) in enumerate(T1_chopped.hops):
        print("----------------")
        for op in hop:
            print(op.table.name)
            print(op.operation_type)
            print(op.condition)
            print(op.new_value)
        print("----------------")
        if T1_chopped.hops_table[i] == "follow":
            follow_table_processor.push_hop(hop)
        elif T1_chopped.hops_table[i] == "user":
            user_table_processor.push_hop(hop)
        elif T1_chopped.hops_table[i] == "tweet":
            tweet_table_processor.push_hop(hop)

        time.sleep(3)