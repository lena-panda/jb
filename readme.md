# How it works

1. Let's assume that we work in Clickhouse database __jb__.
2. Every day we receive a new batch with events, we join it to the __jb.user_action_raw__ table with a new __load_timestamp__ value.
3. Assume we have some kind of configured scheduler that triggers the creation of new patches.
4. After the new patch appears in __jb.user_action_raw__, the program __main.py__ runs the clickhouse-script using [clickhouse_driver](https://clickhouse-driver.readthedocs.io/en/latest/) library.
5. The script `sql/user_session_dds.sql` processes and adds __user_session_id__ to new rows, according to the rules:
    - Session can only start with one of the user's actions: __event_id IN ('a', 'b', 'c')__;
    - Session ends if there are no user actions for 5 minutes;
    - If non-user events are located between the beginning and end of the session (including 5 minutes of inactivity), they are included in the session;
    - In order to extend previously started sessions we take already processed data for the five minutes preceding the minimum date in the new batch;
    - In the end we insert a result to the DDS-table __jb.user_session_dds__.
6. For local debugging I generated random data in __jb.user_action_raw__ using __tables_prepare.py__
7. For further visual analysis I created the Data Mart __jb.user_session_dm__, which has a column with duration of sessions.
8. For dashboard and charts creation I used Yandex Datalens: https://datalens.yandex/czksyp65wbf40
