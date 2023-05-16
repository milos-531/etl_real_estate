from transformer import Transformer

if __name__ == "__main__":
    input_file = "raw_output.csv"
    output_file = "processed_output.csv"
    
    Transformer.run(input_file, output_file)
