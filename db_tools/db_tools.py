from sqlalchemy import create_engine, text, column, table


class DBTools:

    @staticmethod
    def get_connection_string():
        username = "postgres"
        password = "postgres"
        port = "5432"
        db_name = "real_estate"
        conn_string = f"postgresql://{username}:{password}@localhost:{port}/{db_name}"
        return conn_string
