import abc
from typing import List, Union
from lib.ops_generate import generate_T1, generate_T2
from lib.db import Table, Operation

import pandas as pd



if __name__ == "__main__":
    tweet_table = Table("tweet.csv")
    user_table = Table("user.csv")
    follow_table = Table("follow.csv")

    T1 = generate_T1(follow_table, user_table)
    for op in T1:
        op.execute()
    follow_table.save()
    user_table.save()

    T2 = generate_T2(follow_table, user_table)
    for op in T2:
        op.execute()
    follow_table.save()
    user_table.save()
