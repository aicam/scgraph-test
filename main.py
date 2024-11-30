import abc
from typing import List, Union

import pandas as pd


class Table:
    header: List[str]
    df: pd.DataFrame

    def __init__(self, file: str):
        self.df = pd.read_csv(file)
        self.header = self.df.columns.tolist()  # get the header of the table

    def insert(self, row: dict):
        self.df = self.df.append(pd.Series(row, index=self.header), ignore_index=True)

    def select(self, condition: dict) -> pd.DataFrame:
        df_copy = self.df.copy()
        df_copy = df_copy[self.df[condition["k"]] == condition["v"]]
        return df_copy

    def update(self, condition: dict, new_value: dict):
        self.df.loc[self.df[condition["k"]] == condition["v"], new_value["k"]] = new_value["v"]

class Operation:
    table: Table
    operation: str
    condition: dict
    new_value: dict

    def __init__(self, table: Table, operation: str, condition: Union[dict, List[dict]], new_value: dict):
        self.table = table
        self.operation = operation
        self.condition = condition
        self.new_value = new_value

    def execute(self):
        if self.operation == "insert":
            self.table.insert(self.new_value)
        elif self.operation == "select":
            return self.table.select(self.condition)
        elif self.operation == "update":
            self.table.update(self.condition, self.new_value)

class Transaction:
    operations: List[Operation]

if __name__ == "__main__":
    tweet_table = Table("tweet.csv")
    user_table = Table("user.csv")
    follow_table = Table("follow.csv")

    print(tweet_table.header)
    print(user_table.select({"k": "user_id", "v": 101}))