from connect import client
from tables_prepare import create_tables, fill_raw_table

need_to_prepare_tables = False


def main():
    if need_to_prepare_tables:
        create_tables()
        fill_raw_table()

    with open('sql/user_session_dds.sql', 'r', encoding='utf-8') as file:
        query_user_session_dds = file.read()
    client.execute(query_user_session_dds)

    with open('sql/user_session_dm.sql', 'r', encoding='utf-8') as file:
        query_user_session_dm = file.read()
    client.execute(query_user_session_dm)


if __name__ == "__main__":
    main()
