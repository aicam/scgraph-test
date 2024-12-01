import abc
import time
from typing import List, Union

from lib.chop import chop_T
from lib.log_config import setup_logging
from lib.masterprocessor import MasterProcessor
from lib.ops_generate import generate_T1, generate_T2, generate_T3, generate_T4, generate_T_err
from lib.db import Table, Operation

import pandas as pd

from lib.slaveprocessor import SlaveProcessor

if __name__ == "__main__":
    setup_logging("db.log")

    tweet_table = Table("tweet.csv")
    user_table = Table("user.csv")
    follow_table = Table("follow.csv")

    tweet_table_processor = SlaveProcessor(tweet_table, "tweet")
    user_table_processor = SlaveProcessor(user_table, "user")
    follow_table_processor = SlaveProcessor(follow_table, "follow")

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

    master_processor = MasterProcessor([tweet_table_processor, user_table_processor, follow_table_processor])
    master_processor.start()

    for _ in range(10):
        T1 = generate_T1(follow_table, user_table)
        T2 = generate_T2(tweet_table, user_table)
        T3 = generate_T3(tweet_table, user_table)
        T4 = generate_T4(tweet_table, user_table)
        T_err = generate_T_err(follow_table, user_table)
        master_processor.push_T(T1)
        master_processor.push_T(T2)
        master_processor.push_T(T3)
        master_processor.push_T(T4)
        master_processor.push_T(T_err)
    time.sleep(2)