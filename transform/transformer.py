import pandas as pd


class Transformer:
    @staticmethod
    def run(input_file, output_file="processed_output.csv"):
        df_raw = Transformer.__read_raw_file(input_file)

        df_filled_na = Transformer.__drop_na(df_raw)
        df_cleaned = Transformer.__clean(df_filled_na)
        df_enriched = Transformer.__enrich(df_cleaned)
        df_dropped_columns = Transformer.__drop_columns(df_enriched)
        df_dropped_columns.to_csv(output_file)

    @staticmethod
    def __drop_columns(df):
        columns_to_keep = ['title', 'type', 'area', 'num_rooms', 'posted_by', 'age', 'condition',
       'equipment', 'heating', 'floor', 'total_floors', 'utilities_cost', 'utilities_currency',
       'payment_period', 'rent_cost', 'rent_currency', 'city', 'location',
       'micro_location', 'street', 'date_posted', 'deposit', 'not_in_house',
       'pet_friendly', 'immediately_available', 'balcony', 'air_conditioning',
       'garrage', 'elevator', 'internet', 'intercom', 'video_surveillance',
       'parking', 'telephone', 'cable_tv', 'not_final_floor', 'for_students', 'basement', 'garden', 'description',
       'url', 'smoking_forbidden']
        df = df[columns_to_keep]
        return df
    @staticmethod
    def __drop_na(df):
        columns_to_consider = ['title', 'area', 'rent_cost', 'rent_currency', 'city', 'date_posted']
        df = df.dropna(subset=columns_to_consider)
        return df

    @staticmethod
    def __read_raw_file(input_file):
        print(input_file)
        df_raw = pd.read_csv(input_file, decimal=',')
        return df_raw

    @staticmethod
    def __clean(df):
        df = Transformer.__parse_area(df)
        df = Transformer.__parse_rent_cost(df)
        df = Transformer.__parse_floor(df)
        df = Transformer.__parse_utilities(df)
        df = Transformer.__parse_date_posted(df)
        df = Transformer.__parse_num_rooms(df)
        
        return df

    @staticmethod
    def __parse_date_posted(df):
        df['date_posted'] = df['date_posted'].astype(str).str[:10]
        return df
    @staticmethod
    def __parse_num_rooms(df):
        df["num_rooms"] = df["num_rooms"].str.replace('+','')
        df["num_rooms"] = df["num_rooms"].astype(float)
        return df
    @staticmethod
    def __enrich(df):
        df['smoking_forbidden'] = (~df['allowed_smoking']) & df['no_smoking']
        return df

    @staticmethod
    def __parse_utilities(df):
        df[["utilities_cost", "utilities_currency"]] = df["utilities_cost"].str.split(" ", expand=True)
        df["utilities_cost"] = df["utilities_cost"].astype(float)        
        return df

    @staticmethod
    def __parse_area(df):
        df[["area", "area_unit"]] = df["area"].str.split(" ", expand=True)
        df["area"] = df["area"].str.replace(',', '.', regex=True).astype(float)
        df["area"] = df["area"].astype(int)
        return df

    @staticmethod
    def __parse_rent_cost(df):
        df["rent_cost"] = df["rent_cost"].str.replace(".", "")
        df["rent_cost"] = df["rent_cost"].astype(int)
        return df

    @staticmethod
    def __parse_floor(df):
        df["floor"] = df["floor"].str.replace("VPR", "0.5")
        df["floor"] = df["floor"].str.replace("PR", "0")
        df["floor"] = df["floor"].str.replace("PSUT", "-0.5")
        df["floor"] = df["floor"].str.replace("SUT", "-0.5")
        df['floor'] = df['floor'].astype(float)
        return df
