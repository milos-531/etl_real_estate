import pandas as pd
from sqlalchemy import create_engine, text, column, table
from db_tools.db_tools import DBTools
import sqlalchemy

class Loader:
    @staticmethod
    def __read_file(input_file="processed_output.csv"):
        df_raw = pd.read_csv(input_file, decimal=",")
        return df_raw

    @staticmethod
    def run(
        input_file="processed_output.csv", table_name="real_estate", pk_column="url"
    ):
        df = Loader.__read_file(input_file)
        conn_string = DBTools.get_connection_string()
        engine = create_engine(conn_string)
        df_new, df_duplicate = Loader.__get_new_and_duplicate_entries(
            df, engine, table_name, pk_column
        )
        if df_new.empty:
            print("No new entries to insert")
        else:
            Loader.__insert_new_entries(df_new, engine, table_name, pk_column)

    @staticmethod
    def __insert_new_entries(df_new, engine, table_name, pk_column):
        response = df_new.to_sql(table_name, engine, if_exists="append", method="multi")
        Loader.__add_primary_key(engine, table_name, pk_column)

        if response > 0:
            print(f"Inserted {response} rows to database")
        else:
            print("Failed to insert to database")

    @staticmethod
    def __get_new_and_duplicate_entries(df, engine, table_name, pk_column):
        if not sqlalchemy.inspect(engine).has_table(table_name): #If table doesn't exist
            return df, None
        existing_urls = Loader.__get_existing(
            engine, tuple(df[pk_column]), table_name, pk_column
        )
        df_new = df[~df[pk_column].isin(existing_urls)]
        df_duplicate = df[df[pk_column].isin(existing_urls)]
        return df_new, df_duplicate
    @staticmethod

    @staticmethod
    def __get_existing(engine, values_to_insert, table_name, column):
        query = f"SELECT DISTINCT {column} from {table_name} WHERE {column} IN :urls"
        query = text(query)
        with engine.connect() as con:
            existing_urls = pd.read_sql_query(
                query, con, params={"urls": values_to_insert}
            )
        return existing_urls[column]

    @staticmethod
    def __add_primary_key(engine, table_name, pk_column):
        with engine.connect() as con:
            command = f'ALTER TABLE {table_name} ADD PRIMARY KEY ("{pk_column}");'
            command = text(command)
            res = con.execute(command)
            x = res
