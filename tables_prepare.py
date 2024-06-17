import pandas as pd
from connect import client
import random
from datetime import datetime, timedelta

drop_user_action_raw_query = '''
DROP TABLE IF EXISTS jb.user_action_raw
'''

create_user_action_raw_query = '''
CREATE TABLE IF NOT EXISTS jb.user_action_raw (
  load_timestamp Nullable(Int32),
  timestamp Nullable(Int32),
  user_id Nullable(Int32),
  event_id Nullable(String),
  product_code Nullable(String)
)
ENGINE = MergeTree()
PARTITION BY (load_timestamp)
ORDER BY (timestamp, user_id, product_code, event_id)
SETTINGS allow_nullable_key=1
'''

drop_user_session_dds_query = '''
DROP TABLE IF EXISTS jb.user_session_dds
'''

create_user_session_dds_query = '''
CREATE TABLE IF NOT EXISTS jb.user_session_dds (
  timestamp Nullable(Int32),
  user_id Nullable(Int32),
  event_id Nullable(String),
  product_code Nullable(String),
  user_session_id Nullable(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(fromUnixTimestamp(timestamp))
ORDER BY (timestamp, user_id, product_code, event_id)
SETTINGS allow_nullable_key=1
'''

fill_raw_query = '''
INSERT INTO jb.user_action_raw (load_timestamp, timestamp, user_id, event_id, product_code) VALUES
'''

drop_user_session_dm_query = '''
DROP TABLE IF EXISTS jb.user_session_dm
'''

create_user_session_dm_query = '''
CREATE TABLE IF NOT EXISTS jb.user_session_dm (
  user_id Nullable(Int32),
  product_code Nullable(String),
  user_session_id Nullable(String),
  start_session_dttm Nullable(DateTime),
  end_session_dttm Nullable(DateTime),
  session_duration_sec Nullable(Int32)
)
ENGINE = MergeTree()
ORDER BY (user_id, product_code, user_session_id)
SETTINGS allow_nullable_key=1
'''

# Raw-table schema for batches
user_action_raw_schema = {
    'load_timestamp': 'Int32',
    'timestamp': 'Int32',
    'user_id': 'Int32',
    'event_id': 'String',
    'product_code': 'String'
}


def create_tables():
    # Create jb.user_action_raw table in ClickHouse
    client.execute(drop_user_action_raw_query)
    client.execute(create_user_action_raw_query)
    # Create jb.user_session_dds table in ClickHouse
    client.execute(drop_user_session_dds_query)
    client.execute(create_user_session_dds_query)
    # Create jb.user_session_dm table in ClickHouse
    client.execute(drop_user_session_dm_query)
    client.execute(create_user_session_dm_query)


def fill_raw_table():
    # Create empty DataFrame
    example_data = pd.DataFrame(columns=user_action_raw_schema.keys())
    # Create some random data
    for day in range(10):
        for i in range(5000):
            row = {
                'load_timestamp': int(datetime.timestamp(datetime.now().replace(second=0, microsecond=0) - timedelta(days=day-1))),
                'timestamp': int(datetime.timestamp(datetime.now().replace(minute=0, hour=0, second=0, microsecond=0) - timedelta(days=day) + timedelta(seconds=random.randrange(1, 86400, 1)))),
                'user_id': random.SystemRandom().choice([1, 3, 5, 7, 9, 11, 15]),
                'event_id': random.SystemRandom().choice(['a', 'b', 'c', 'x', 'y', 'z']),
                'product_code': random.SystemRandom().choice(['datagrip', 'pycharm', 'datalore']),
            }
            example_data = pd.concat([example_data, pd.DataFrame([row])], ignore_index=True)

    example_data.sort_values(by=['load_timestamp', 'timestamp', 'user_id', 'product_code', 'event_id'])
    client.execute(
        'TRUNCATE TABLE IF EXISTS jb.user_action_raw'
    )
    client.execute(fill_raw_query, example_data.values.tolist())
    print('Table user_action_raw with example data is ready')
