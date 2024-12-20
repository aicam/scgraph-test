from typing import List, Union, Optional

import pandas as pd


class Table:
    name: str
    header: List[str]
    df: pd.DataFrame
    file: str

    def __init__(self, file: str):
        self.df = pd.read_csv(file)
        self.file = file
        self.name = file.split(".")[0]
        self.header = self.df.columns.tolist()  # get the header of the table

    def insert(self, row: dict):
        self.df = pd.concat([self.df, pd.DataFrame([row])], ignore_index=True)

    def select(self, condition: dict) -> pd.DataFrame:
        df_copy = self.df.copy()
        df_copy = df_copy[self.df[condition["k"]] == condition["v"]]
        return df_copy

    def update(self, condition: dict, new_value: dict):
        self.df.loc[self.df[condition["k"]] == condition["v"], new_value["k"]] = new_value["v"]

    def delete(self, condition: dict):
        self.df = self.df.drop(self.df[self.df[condition["k"]] != condition["v"]].index[0])

    def save(self):
        self.df.to_csv(self.file, index=False)

class Operation:
    table: Table
    operation_type: str
    condition: Optional[dict] = None
    new_value: Optional[dict] = None
    executed: bool = False

    def __init__(self, table: Table, operation: str, condition: Optional[Union[dict, List[dict]]] = None, new_value: Optional[dict] = None):
        self.table = table
        self.operation_type = operation
        self.condition = condition
        if condition is None:
            self.condition = {"k": None, "v": None}
        self.new_value = new_value

    def execute(self):
        if self.operation_type == "insert":
            self.table.insert(self.new_value)
        elif self.operation_type == "select":
            self.table.select(self.condition)
        elif self.operation_type == "update":
            self.table.update(self.condition, self.new_value)
        self.executed = True

