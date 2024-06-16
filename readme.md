# How it works

1. Let's assume that we are working in Clickhouse database __jb__.
2. Every day we receive a new patch with events, which unions to the __jb.user_action_raw__ table with a new __load_timestamp__ value.
3. After the new patch appears, we can run the clickhouse-script (suppose we have some kind of configured scheduler that triggers the creation of new patches) using [clickhouse_driver](https://clickhouse-driver.readthedocs.io/en/latest/) library.
4. The script `sql/user_session_dds.sql` processes and adds __user_session_id__ to new rows, according to the rules:
    - Session can only start with one of the user's actions: __event_id IN ('a', 'b', 'c')__;
    - Session ends if there are no user actions for 5 minutes;
    - If non-user (system) events are located between the beginning and end of the session (including 5 minutes of inactivity), they are included in the session;
    - During processing, we capture already processed data in order to extend previously started sessions;
    - In the end we insert a result to the DDS-table __jb.user_session_dds__.
5. For local debugging I generated random data in __jb.user_action_raw__ using __tables_prepare.py__
6. For further visual analysis I created the data mart __jb.user_session_dm__, which has a column with duration of sessions.
7. For dashboard and charts creation I used Yandex Datalens: https://datalens.yandex/czksyp65wbf40