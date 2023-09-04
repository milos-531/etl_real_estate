from loader import Loader

if __name__ == "__main__":
    input_file = "processed_output.csv"
    table_name = "real_estate"
    Loader.run(input_file, table_name)
