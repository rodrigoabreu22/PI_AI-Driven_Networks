import pandas as pd
import csv


# Input and output file paths
input_file = 'NF-UNSW-NB15-v3.csv'
output_file = 'data.csv'

# Columns to keep
columns_to_keep = [
    "FLOW_START_MILLISECONDS","FLOW_END_MILLISECONDS","IPV4_SRC_ADDR","L4_SRC_PORT","IPV4_DST_ADDR","L4_DST_PORT",
    "PROTOCOL","L7_PROTO","IN_BYTES","IN_PKTS","OUT_BYTES","OUT_PKTS","TCP_FLAGS","CLIENT_TCP_FLAGS",
    "SERVER_TCP_FLAGS","FLOW_DURATION_MILLISECONDS","DURATION_IN","DURATION_OUT","MIN_TTL","MAX_TTL",
    "LONGEST_FLOW_PKT","SHORTEST_FLOW_PKT","MIN_IP_PKT_LEN","MAX_IP_PKT_LEN","RETRANSMITTED_IN_BYTES",
    "RETRANSMITTED_IN_PKTS","RETRANSMITTED_OUT_BYTES","RETRANSMITTED_OUT_PKTS","SRC_TO_DST_AVG_THROUGHPUT",
    "DST_TO_SRC_AVG_THROUGHPUT","NUM_PKTS_UP_TO_128_BYTES","NUM_PKTS_128_TO_256_BYTES",
    "NUM_PKTS_256_TO_512_BYTES","NUM_PKTS_512_TO_1024_BYTES","NUM_PKTS_1024_TO_1514_BYTES",
    "TCP_WIN_MAX_IN","TCP_WIN_MAX_OUT","ICMP_TYPE","ICMP_IPV4_TYPE","DNS_QUERY_ID","DNS_QUERY_TYPE",
    "DNS_TTL_ANSWER","FTP_COMMAND_RET_CODE","Label","Attack"
]

# Read CSV
df = pd.read_csv(input_file)

# Keep only specified columns
df = df[[col for col in columns_to_keep if col in df.columns]]

# Truncate the first 2 columns to the first 10 digits
for col in ["FLOW_START_MILLISECONDS", "FLOW_END_MILLISECONDS"]:
    if col in df.columns:
        df[col] = df[col].astype(str).str.slice(0, 10)

# Save the modified DataFrame
df.to_csv(output_file, index=False)

print("CSV processed and saved to", output_file)


file_path = 'data.csv'



with open(file_path, newline='') as csvfile:
    reader = csv.reader(csvfile)
    row_lengths = [len(row) for row in reader]

# Check if all rows have the same length
if all(length == row_lengths[0] for length in row_lengths):
    print("✅ All rows have the same number of columns:", row_lengths[0])
else:
    print("❌ Inconsistent row lengths found:")
    for i, length in enumerate(row_lengths):
        if length != row_lengths[0]:
            print(f"Row {i + 1} has {length} columns")

