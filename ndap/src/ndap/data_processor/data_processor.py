import subprocess
import pandas as pd
import time
import os
import shutil

def run_nprobe(input_pcap):
    command = [
        "nprobe", "-i", input_pcap, "-P", "./features", "--csv-separator", ",", "--dont-reforge-timestamps",
        "-V", "9", "-T", 
        "%FLOW_START_MILLISECONDS %FLOW_END_MILLISECONDS %IPV4_SRC_ADDR %L4_SRC_PORT %IPV4_DST_ADDR %L4_DST_PORT %PROTOCOL %L7_PROTO %IN_BYTES %IN_PKTS %OUT_BYTES %OUT_PKTS %TCP_FLAGS %CLIENT_TCP_FLAGS %SERVER_TCP_FLAGS %FLOW_DURATION_MILLISECONDS %DURATION_IN %DURATION_OUT %MIN_TTL %MAX_TTL %LONGEST_FLOW_PKT %SHORTEST_FLOW_PKT %MIN_IP_PKT_LEN %MAX_IP_PKT_LEN %SRC_TO_DST_SECOND_BYTES %DST_TO_SRC_SECOND_BYTES %RETRANSMITTED_IN_BYTES %RETRANSMITTED_IN_PKTS %RETRANSMITTED_OUT_BYTES %RETRANSMITTED_OUT_PKTS %SRC_TO_DST_AVG_THROUGHPUT %DST_TO_SRC_AVG_THROUGHPUT %NUM_PKTS_UP_TO_128_BYTES %NUM_PKTS_128_TO_256_BYTES %NUM_PKTS_256_TO_512_BYTES %NUM_PKTS_512_TO_1024_BYTES %NUM_PKTS_1024_TO_1514_BYTES %TCP_WIN_MAX_IN %TCP_WIN_MAX_OUT %ICMP_TYPE %ICMP_IPV4_TYPE %DNS_QUERY_ID %DNS_QUERY_TYPE %DNS_TTL_ANSWER %FTP_COMMAND_RET_CODE"
    ]
    
    subprocess.run(command, stderr=subprocess.PIPE, text=True)

def move_csv_files():
    output_file = "temp.flows"
    flow_files = []

    for root, _, files in os.walk("./features"):
        for f in files:
            if f.endswith(".flows"):
                flow_files.append(os.path.join(root, f))

    flow_files.sort()

    with open(output_file, "w") as outfile:
        first_file = True
        for file_path in flow_files:
            with open(file_path, "r") as infile:
                lines = infile.readlines()
                if len(lines) <= 1:
                    continue  # skip empty files
                start_index = 0

                if first_file:
                    # Write header as-is
                    outfile.write(lines[0])
                    first_file = False
                    start_index = 1  # Start from data rows

                for line in lines[start_index:]:
                    fields = line.strip().split(",")
                    if len(fields) < 2:
                        continue  # skip malformed lines
                    fields[0] = fields[0][:10]
                    fields[1] = fields[1][:10]
                    outfile.write(",".join(fields) + "\n")

    shutil.rmtree("./features")
    os.makedirs("./features")
def analyze_output(csv_file):
    try:
        df = pd.read_csv(csv_file, delimiter=",")
        print("Data Shape:", df.shape)

    except Exception as e:
        print("Error loading CSV:", e)

def addGT(csv_file):
    gt_df = pd.read_csv("NUSW-NB15_GT.csv", usecols=[
        'Start time', 'Last time', 'Protocol',
        'Source IP', 'Source Port',
        'Destination IP', 'Destination Port',
        'Attack category'
    ])

    # Rename GT columns to match temp.flows style
    gt_df.rename(columns={
        'Start time': 'FLOW_START_MILLISECONDS',
        'Last time': 'FLOW_END_MILLISECONDS',
        'Protocol': 'PROTOCOL',
        'Source IP': 'IPV4_SRC_ADDR',
        'Source Port': 'L4_SRC_PORT',
        'Destination IP': 'IPV4_DST_ADDR',
        'Destination Port': 'L4_DST_PORT'
    }, inplace=True)


    # Convert all matching columns to string to ensure consistent merge
    match_cols = [
    'FLOW_START_MILLISECONDS', 'FLOW_END_MILLISECONDS', 'PROTOCOL',
    'IPV4_SRC_ADDR', 'L4_SRC_PORT', 'IPV4_DST_ADDR', 'L4_DST_PORT'
    ]

    gt_df[match_cols] = gt_df[match_cols].astype(str)

    # Load temp flows
    temp_df = pd.read_csv("temp.flows", sep=",")
    print(temp_df.columns.tolist())
    temp_df[match_cols] = temp_df[match_cols].astype(str)

    # Merge on matching columns
    merged = temp_df.merge(gt_df, on=match_cols, how='left')

    # Set 'target' and fill missing attack categories
    merged['target'] = merged['Attack category'].notna().astype(int)
    merged['attack_category'] = merged['Attack category'].fillna('Benign')
    merged.drop(columns=['Attack category'], inplace=True)

    # Save to extracted.flows with same format as temp.flows
    merged.to_csv("extracted.flows", index=False)
    # Remove temp.flows
    os.remove(csv_file)


if __name__ == "__main__":
    run_nprobe("1.pcap")
    print("Waiting for nprobe to finish...")
    move_csv_files()
    print("CSV files moved and merged.")
    analyze_output("temp.flows")
    print("Analyzing output...")
    addGT("temp.flows")
    print("Ground truth added to extracted.flows.")



