import subprocess
import pandas as pd
import time
import os
import shutil
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import logging
import json

TOPIC = "PROCESSED_NETWORK_DATA"
BROKER = 'kafka:9092'

def create_topic(topic_name, broker, num_partitions=1, replication_factor=1):
    """
    Ensure the Kafka topic exists; create it if it does not.
    """
    admin_client = None
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=broker)
        
        # Check if the topic exists
        existing_topics = admin_client.list_topics()
        if topic_name in existing_topics:
            logging.info(f"Topic '{topic_name}' already exists.")
        else:
            # Create the topic
            topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            admin_client.create_topics([topic])

    except Exception as e:
        logging.error(f"Failed to create topic '{topic_name}': {e}")
    finally:
        if admin_client is not None:
            admin_client.close()

def create_kafka_producer():
    """Creates a Kafka producer for sending raw packet data."""
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def send_to_kafka(producer, topic, data):
    """Sends data to Kafka."""
    try:
        producer.send(topic, data)
        producer.flush()

    except Exception as e:
        logging.error(f"Error sending packet: {e}")


def send_data():
    producer = create_kafka_producer()
    data = pd.read_csv("extracted.flows", sep=",", header=0)

    for row_dict in data.to_dict(orient='records'):
        send_to_kafka(producer, TOPIC, row_dict)

    os.remove("extracted.flows")

def run_nprobe(input_pcap):
    command = [
        "nprobe", "-i", input_pcap, "-P", "./features", "--csv-separator", ",", "--dont-reforge-timestamps",
        "-V", "9", "-T", 
        "%FLOW_START_MILLISECONDS %FLOW_END_MILLISECONDS %IPV4_SRC_ADDR %L4_SRC_PORT %IPV4_DST_ADDR %L4_DST_PORT %PROTOCOL %L7_PROTO %IN_BYTES %IN_PKTS %OUT_BYTES %OUT_PKTS %TCP_FLAGS %CLIENT_TCP_FLAGS %SERVER_TCP_FLAGS %FLOW_DURATION_MILLISECONDS %DURATION_IN %DURATION_OUT %MIN_TTL %MAX_TTL %LONGEST_FLOW_PKT %SHORTEST_FLOW_PKT %MIN_IP_PKT_LEN %MAX_IP_PKT_LEN %RETRANSMITTED_IN_BYTES %RETRANSMITTED_IN_PKTS %RETRANSMITTED_OUT_BYTES %RETRANSMITTED_OUT_PKTS %SRC_TO_DST_AVG_THROUGHPUT %DST_TO_SRC_AVG_THROUGHPUT %NUM_PKTS_UP_TO_128_BYTES %NUM_PKTS_128_TO_256_BYTES %NUM_PKTS_256_TO_512_BYTES %NUM_PKTS_512_TO_1024_BYTES %NUM_PKTS_1024_TO_1514_BYTES %TCP_WIN_MAX_IN %TCP_WIN_MAX_OUT %ICMP_TYPE %ICMP_IPV4_TYPE %DNS_QUERY_ID %DNS_QUERY_TYPE %DNS_TTL_ANSWER %FTP_COMMAND_RET_CODE"
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
                    outfile.write(lines[0])
                    first_file = False
                    start_index = 1
                else:
                    # For subsequent files, always skip first line (header)
                    start_index = 1

                for line in lines[start_index:]:
                    if "FLOW_START" in line or "IPV4_SRC_ADDR" in line:
                        continue  # skip accidental headers
                    fields = line.strip().split(",")
                    if len(fields) < 2:
                        continue  # skip malformed lines
                    fields[0] = fields[0][:10]
                    fields[1] = fields[1][:10]
                    outfile.write(",".join(fields) + "\n")

    shutil.rmtree("./features")
    os.makedirs("./features")

def addGT(csv_file):
    protocol_map = {
        'tcp': '6',
        'udp': '17',
        'icmp': '1',
        'icmpv6': '58'
    }

    gt_df = pd.read_csv("NUSW-NB15_GT.csv", usecols=[
        'Start time', 'Last time', 'Protocol',
        'Source IP', 'Source Port',
        'Destination IP', 'Destination Port',
        'Attack category'
    ])

    # Standardize protocol
    gt_df['Protocol'] = gt_df['Protocol'].str.lower().map(protocol_map)

    # Rename columns to match flow file
    gt_df.rename(columns={
        'Start time': 'FLOW_START_MILLISECONDS',
        'Last time': 'FLOW_END_MILLISECONDS',
        'Protocol': 'PROTOCOL',
        'Source IP': 'IPV4_SRC_ADDR',
        'Source Port': 'L4_SRC_PORT',
        'Destination IP': 'IPV4_DST_ADDR',
        'Destination Port': 'L4_DST_PORT'
    }, inplace=True)

    match_cols = [
        'FLOW_START_MILLISECONDS', 'FLOW_END_MILLISECONDS', 'PROTOCOL',
        'IPV4_SRC_ADDR', 'L4_SRC_PORT', 'IPV4_DST_ADDR', 'L4_DST_PORT'
    ]

    gt_df[match_cols] = gt_df[match_cols].astype(str)

    temp_df = pd.read_csv(csv_file, sep=",")
    temp_df[match_cols] = temp_df[match_cols].astype(str)

    merged = temp_df.merge(gt_df, on=match_cols, how='left')

    # Rename & add label columns
    merged['Label'] = merged['Attack category'].notna().astype(int)
    merged['Attack'] = merged['Attack category'].fillna('Benign')
    merged.drop(columns=['Attack category'], inplace=True)

    # Save cleaned output
    merged.to_csv("extracted.flows", index=False)
    os.remove(csv_file)

def processor_main(file):
    create_topic(TOPIC, BROKER)
    run_nprobe(file)
    move_csv_files()
    addGT("temp.flows")
    send_data() 