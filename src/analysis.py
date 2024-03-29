import logging
from src.logging_config import configure_logging


configure_logging()


def analysis(conn):
    if conn is not None:
        # Who are the top 10 users with respect to the number of songs listened to?

        first_query = """SELECT user_name, COUNT(id) as total_listens
        FROM listens
        GROUP BY user_name
        ORDER BY total_listens DESC
        LIMIT 10;"""

        logging.info(conn.query(first_query))

        second_query = """SELECT COUNT(DISTINCT user_name) as users_count
        FROM listens
        WHERE listened_at::date = '2019-03-01';"""

        logging.info(conn.query(second_query))

        third_query = """WITH first_listen AS (
            SELECT user_name, MIN(listened_at) as first_listen_time
            FROM listens
            GROUP BY user_name
        )
        SELECT FL.user_name, FL.first_listen_time, L.track_name
        FROM first_listen FL
        JOIN listens L ON FL.user_name = L.user_name AND FL.first_listen_time = L.listened_at
        ORDER BY FL.user_name ;"""

        logging.info(conn.query(third_query))

        fourth_query = """WITH ListensPerUser AS (
            SELECT user_name, listened_at::date as date, COUNT(id) as number_of_listens
            FROM listens
            GROUP BY user_name, listened_at::date
        ),
        RankedListens AS (
            SELECT user_name, date, number_of_listens,
                   Row_number() OVER (PARTITION BY user_name ORDER BY number_of_listens DESC) as row_number
            FROM ListensPerUser
        )
        SELECT user_name, number_of_listens, date
        FROM RankedListens
        WHERE row_number <= 3
        ORDER BY user_name, number_of_listens DESC;"""

        logging.info(conn.query(fourth_query))

        fifth_query = """WITH active_users AS (
            SELECT user_name, listened_at::date as listen_date
            FROM listens
            GROUP BY user_name, listen_date
            HAVING listen_date BETWEEN listen_date - INTERVAL '6 days' AND listen_date
        )
        SELECT listen_date, COUNT(DISTINCT user_name) as number_active_users,
               (COUNT(DISTINCT user_name) * 100.0 / (SELECT COUNT(DISTINCT user_name) FROM listens)) as percentage_active_users
        FROM active_users
        GROUP BY listen_date
        ORDER BY listen_date;"""

        logging.info(conn.query(fifth_query))

        conn.close()