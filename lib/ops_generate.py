from datetime import datetime
from typing import List

from lib.db import Table, Operation


def generate_T1(follow: Table, user: Table) -> List[Operation]:
    operations = []

    # Step 1: INSERT INTO follow VALUES(19, 1, NOW())
    insert_operation = Operation(
        table=follow,
        operation="insert",
        condition=None,
        new_value={
            "follower_id": 19,
            "following_id": 1,
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )
    operations.append(insert_operation)

    # Step 2: UPDATE user SET num_follower = num_follower + 1 WHERE user_id=19
    update_follower_operation = Operation(
        table=user,
        operation="update",
        condition={"k": "user_id", "v": 19},
        new_value={"k": "num_follower", "v": user.df.loc[user.df["user_id"] == 19, "num_follower"].iloc[0] + 1}
    )
    operations.append(update_follower_operation)

    # Step 3: UPDATE user SET num_following = num_following + 1 WHERE user_id=1
    update_following_operation = Operation(
        table=user,
        operation="update",
        condition={"k": "user_id", "v": 1},
        new_value={"k": "num_following", "v": user.df.loc[user.df["user_id"] == 1, "num_following"].iloc[0] + 1}
    )
    operations.append(update_following_operation)

    # Step 4: SELECT FROM follow WHERE follower_id = 1
    select_follower_operation = Operation(
        table=follow,
        operation="select",
        condition={"k": "follower_id", "v": 1},
        new_value=None
    )
    operations.append(select_follower_operation)

    # Step 5: SELECT FROM follow WHERE following_id = 1
    select_following_operation = Operation(
        table=follow,
        operation="select",
        condition={"k": "following_id", "v": 1},
        new_value=None
    )
    operations.append(select_following_operation)

    return operations

def generate_T2(follow: Table, user: Table) -> List[Operation]:
    operations = []

    # Step 1: INSERT INTO follow VALUES(19, 22, NOW())
    insert_operation = Operation(
        table=follow,
        operation="insert",
        condition=None,
        new_value={
            "follower_id": 19,
            "following_id": 22,
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )
    operations.append(insert_operation)

    # Step 2: UPDATE user SET num_follower = num_follower + 1 WHERE user_id=22
    update_follower_operation = Operation(
        table=user,
        operation="update",
        condition={"k": "user_id", "v": 22},
        new_value={"k": "num_follower", "v": user.df.loc[user.df["user_id"] == 22, "num_follower"].iloc[0] + 1}
    )
    operations.append(update_follower_operation)

    # Step 3: UPDATE user SET num_following = num_following + 1 WHERE user_id=19
    update_following_operation = Operation(
        table=user,
        operation="update",
        condition={"k": "user_id", "v": 19},
        new_value={"k": "num_following", "v": user.df.loc[user.df["user_id"] == 19, "num_following"].iloc[0] + 1}
    )
    operations.append(update_following_operation)

    # Step 4: SELECT FROM follow WHERE follower_id = 19
    select_follower_operation = Operation(
        table=follow,
        operation="select",
        condition={"k": "follower_id", "v": 19},
        new_value=None
    )
    operations.append(select_follower_operation)

    # Step 5: SELECT FROM follow WHERE following_id = 19
    select_following_operation = Operation(
        table=follow,
        operation="select",
        condition={"k": "following_id", "v": 19},
        new_value=None
    )
    operations.append(select_following_operation)

    return operations