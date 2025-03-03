from mysql.connector import connect, Error

class MySqlHelper:
    """Class for saving data to a MySQL database."""

    def __init__(self, db_config):
        self.db_config = db_config

    from mysql.connector import connect, Error

    class MySqlHelper:
        """Class for saving data to a MySQL database."""

        def __init__(self, db_config):
            self.db_config = db_config

        def save_data(self, data_list, table):
            """
            Save data to the specified table in the MySQL database.
            :param data_list: List of movie dictionaries containing movie information
            :param table: The table name in the database where data should be stored
            """
            try:
                connection = connect(**self.db_config)
                if connection.is_connected():
                    print("Connected to database")

                insert_query = """
                    INSERT INTO movies (
                        title, rating, num_raters, quote, director,
                        actors, release_date, genres, link
                    ) VALUES (
                        %(title)s, %(rating)s, %(num_raters)s, %(quote)s, 
                        %(director)s, %(actors)s, %(release_date)s, %(genres)s, %(link)s
                    )
                """

                with connection.cursor() as cursor:
                    # 使用字典参数执行多行插入
                    cursor.executemany(insert_query, data_list)
                    connection.commit()
                    print(f"Successfully inserted {len(data_list)} records")

            except Error as e:
                print(f"Database operation failed: {e}")
            finally:
                if connection and connection.is_connected():
                    connection.close()
                    print("Database connection closed")
