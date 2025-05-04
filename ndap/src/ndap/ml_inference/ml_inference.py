import pandas as pd

# Input and output file paths
input_file = 'unseen_data.csv'          # or your chosen input file
output_file = 'data_clean.csv'   # will save cleaned data here

# Load the CSV
df = pd.read_csv(input_file)

# Drop the columns if they exist
df = df.drop(columns=['Label', 'Attack'], errors='ignore')

# Save to new CSV
df.to_csv(output_file, index=False)

print(f"Saved cleaned data to {output_file}")
