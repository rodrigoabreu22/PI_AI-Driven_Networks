import csv

# Files
packets_file = "pcap.csv"
flow_file = "flow.csv"  # Your given CSV file
output_file = "flow_modified.csv"

# Step 1: Read timestamps from packets.csv
packets_timestamps = []
with open(packets_file, newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # timestamps are float, but flow expects int milliseconds, so convert to int
        ts = int(float(row['timestamp']))
        packets_timestamps.append(ts)

# Step 2: Read flow.csv and replace timestamps with packets timestamps
flow_rows = []
with open(flow_file, newline='') as f:
    reader = csv.DictReader(f)
    headers = reader.fieldnames
    for i, row in enumerate(reader):
        if i < len(packets_timestamps):
            ts = packets_timestamps[i]
            row['FLOW_START_MILLISECONDS'] = str(ts)
            row['FLOW_END_MILLISECONDS'] = str(ts)
        flow_rows.append(row)

# Step 3: Write modified flow data to new CSV
with open(output_file, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=headers)
    writer.writeheader()
    writer.writerows(flow_rows)

print(f"Modified flow data saved to {output_file}")
