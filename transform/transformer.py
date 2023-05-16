import pandas as pd
from transform.validator import Validator


class Transformer:
    @staticmethod
    def run(input_file='raw_output.csv', output_file="processed_output.csv"):
        df_raw = Transformer.__read_raw_file(input_file)
        if not Validator.is_valid(df_raw):
            raise ValueError
        df_filled_na = Transformer.__fill_na(df_raw)
        df_cleaned = Transformer.__clean(df_filled_na)
        df_enriched = Transformer.__enrich(df_cleaned)
        Transformer.__write(df_enriched, output_file)

    @staticmethod
    def __fill_na(df_raw):
        pass
    @staticmethod
    def __read_raw_file(input_file):
        df_raw = pd.read_csv(input_file)
        return df_raw

    @staticmethod
    def __clean(df_filled_na):
        pass

    @staticmethod
    def __enrich(df_cleaned):
        pass

    @staticmethod
    def __write(df_enriched, output_file):
        pass
