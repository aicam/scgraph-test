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

def generate_T2(tweet: Table, user: Table) -> List[Operation]:
    operations = []

    # Step 1: UPDATE tweet SET num_likes = num_likes + 1 WHERE tweet_id = 22
    update_tweet_likes_operation = Operation(
        table=tweet,
        operation="update",
        condition={"k": "tweet_id", "v": 22},
        new_value={"k": "num_likes", "v": tweet.df.loc[tweet.df["tweet_id"] == 22, "num_likes"].iloc[0] + 1}
    )
    operations.append(update_tweet_likes_operation)

    # Step 2: SELECT * FROM tweet WHERE tweet_id = 22
    select_tweet_operation = Operation(
        table=tweet,
        operation="select",
        condition={"k": "tweet_id", "v": 2},
        new_value=None
    )
    operations.append(select_tweet_operation)

    # Step 3: UPDATE user SET num_likes = num_likes + 1 WHERE user_id = 1
    update_user_likes_operation = Operation(
        table=user,
        operation="update",
        condition={"k": "user_id", "v": 1},
        new_value={"k": "num_likes", "v": user.df.loc[user.df["user_id"] == 1, "num_likes"].iloc[0] + 1}
    )
    operations.append(update_user_likes_operation)

    return operations

def generate_T3(tweet: Table, user: Table) -> List[Operation]:
    operations = []

    # Step 1: INSERT INTO tweet VALUES(58, "Hi", NOW(), 15)
    insert_tweet_operation = Operation(
        table=tweet,
        operation="insert",
        condition=None,
        new_value={
            "tweet_id": 58,
            "content": "Hi",
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "user_id": 15,
            "num_likes": 0,
            "num_unlikes": 0
        }
    )
    operations.append(insert_tweet_operation)

    # Step 2: SELECT * FROM tweet WHERE user_id = 15
    select_tweets_by_user_operation = Operation(
        table=tweet,
        operation="select",
        condition={"k": "user_id", "v": 15},
        new_value=None
    )
    operations.append(select_tweets_by_user_operation)

    # Step 3: UPDATE user SET num_tweets = num_tweets + 1 WHERE user_id = 15
    update_user_tweets_operation = Operation(
        table=user,
        operation="update",
        condition={"k": "user_id", "v": 15},
        new_value={"k": "num_tweets", "v": user.df.loc[user.df["user_id"] == 15, "num_tweets"].iloc[0] + 1}
    )
    operations.append(update_user_tweets_operation)

    return operations

def generate_T4(tweet: Table, user: Table) -> List[Operation]:
    operations = []

    # Step 1: UPDATE tweet SET num_unlikes = num_unlikes + 1 WHERE tweet_id = 58
    update_tweet_dislikes_operation = Operation(
        table=tweet,
        operation="update",
        condition={"k": "tweet_id", "v": 58},
        new_value={"k": "num_dislikes", "v": tweet.df.loc[tweet.df["tweet_id"] == 58, "num_dislikes"].iloc[0] + 1}
    )
    operations.append(update_tweet_dislikes_operation)

    # Step 2: SELECT * FROM tweet WHERE tweet_id = 58
    select_tweet_operation = Operation(
        table=tweet,
        operation="select",
        condition={"k": "tweet_id", "v": 3},
        new_value=None
    )
    operations.append(select_tweet_operation)

    # Step 3: UPDATE user SET num_dislikes = num_dislikes + 1 WHERE user_id = 1
    update_user_dislikes_operation = Operation(
        table=user,
        operation="update",
        condition={"k": "user_id", "v": 1},
        new_value={"k": "num_dislikes", "v": user.df.loc[user.df["user_id"] == 1, "num_dislikes"].iloc[0] + 1}
    )
    operations.append(update_user_dislikes_operation)

    return operations

def generate_T_err(follow: Table, user: Table) -> List[Operation]:
    operations = []

    # Step 1: INSERT INTO follow VALUES(19, 1, NOW())
    insert_operation = Operation(
        table=follow,
        operation="insert",
        condition=None,
        new_value={
            "follower_id": 1,
            "following_id": 19,
            "datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )
    operations.append(insert_operation)

    # Step 2: UPDATE user SET num_follower = num_follower + 1 WHERE user_id=19
    update_follower_operation = Operation(
        table=user,
        operation="update",
        condition={"k": "user_id", "v": 1},
        new_value={"k": "num_follower", "v": user.df.loc[user.df["user_id"] == 1, "num_follower"].iloc[0] + 1}
    )
    operations.append(update_follower_operation)

    # Step 3: UPDATE user SET num_following = num_following + 1 WHERE user_id=1
    update_following_operation = Operation(
        table=user,
        operation="update",
        condition={"k": "user_id", "v": 19},
        new_value={"k": "num_following", "v": user.df.loc[user.df["user_id"] == 19, "num_following"].iloc[0] + 1}
    )
    operations.append(update_following_operation)

    # Step 4: SELECT FROM follow WHERE follower_id = 1
    select_follower_operation = Operation(
        table=follow,
        operation="select",
        condition={"k": "follower_id", "v": 19},
        new_value=None
    )
    operations.append(select_follower_operation)

    # Step 5: SELECT FROM follow WHERE following_id = 1
    select_following_operation = Operation(
        table=follow,
        operation="select",
        condition={"k": "following_id", "v": 19},
        new_value=None
    )
    operations.append(select_following_operation)

    return operations