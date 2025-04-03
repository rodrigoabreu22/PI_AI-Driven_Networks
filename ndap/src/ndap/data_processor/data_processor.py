import subprocess
import pandas as pd
import time
from influxdb_client import InfluxDBClient
import pandas as pd



# Change to match your InfluxDB settings
INFLUXDB_URL = "http://influxdb_container_name:8086"
TOKEN = "your_token"
ORG = "your_org"
BUCKET = "ground_truth"

client = InfluxDBClient(url=INFLUXDB_URL, token=TOKEN, org=ORG)
query_api = client.query_api()

def run_nprobe(input_pcap):
    time_now = time.strftime("%Y/%m/%d/%H/%M")
    year, month, day, hour, minute = time_now.split("/")
    command = [
        "nprobe", "-i", input_pcap, "-P", "./features", "--csv-separator", ",",
        "-V", "9", "-T", 
        "%IPV4_SRC_ADDR %L4_SRC_PORT %IPV4_DST_ADDR %L4_DST_PORT %PROTOCOL %L7_PROTO %IN_BYTES %IN_PKTS %OUT_BYTES %OUT_PKTS %TCP_FLAGS %CLIENT_TCP_FLAGS %SERVER_TCP_FLAGS %FLOW_DURATION_MILLISECONDS %DURATION_IN %DURATION_OUT %MIN_TTL %MAX_TTL %LONGEST_FLOW_PKT %SHORTEST_FLOW_PKT %MIN_IP_PKT_LEN %MAX_IP_PKT_LEN %SRC_TO_DST_SECOND_BYTES %DST_TO_SRC_SECOND_BYTES %RETRANSMITTED_IN_BYTES %RETRANSMITTED_IN_PKTS %RETRANSMITTED_OUT_BYTES %RETRANSMITTED_OUT_PKTS %SRC_TO_DST_AVG_THROUGHPUT %DST_TO_SRC_AVG_THROUGHPUT %NUM_PKTS_UP_TO_128_BYTES %NUM_PKTS_128_TO_256_BYTES %NUM_PKTS_256_TO_512_BYTES %NUM_PKTS_512_TO_1024_BYTES %NUM_PKTS_1024_TO_1514_BYTES %TCP_WIN_MAX_IN %TCP_WIN_MAX_OUT %ICMP_TYPE %ICMP_IPV4_TYPE %DNS_QUERY_ID %DNS_QUERY_TYPE %DNS_TTL_ANSWER %FTP_COMMAND_RET_CODE"
    ]
    
    subprocess.run(command, stderr=subprocess.PIPE, text=True)
    analyze_output("features/" + year +"/"+ month +"/"+ day +"/"+ hour +"/"+ minute+ ".flows")

def analyze_output(csv_file):
    try:
        df = pd.read_csv(csv_file, delimiter=",")
        print("Data Shape:", df.shape)

    except Exception as e:
        print("Error loading CSV:", e)

if __name__ == "__main__":
    run_nprobe("1.pcap")



def addGT(csv_file):
    df = pd.read_csv(csv_file)

    labels = []
    attacks = []

    for _, row in df.iterrows():
        query = f'''
        from(bucket: "{BUCKET}")
        |> range(start: -30d)
        |> filter(fn: (r) => r.IPV4_SRC_ADDR == "{row['IPV4_SRC_ADDR']}" 
            and r.L4_SRC_PORT == "{row['L4_SRC_PORT']}"
            and r.IPV4_DST_ADDR == "{row['IPV4_DST_ADDR']}"
            and r.IN_BYTES == "{row['IN_BYTES']}"
            and r.IN_PKTS == "{row['IN_PKTS']}"
            and r.OUT_BYTES == "{row['OUT_BYTES']}"
            and r.OUT_PKTS == "{row['OUT_PKTS']}")
        |> limit(n:1)
        '''

        tables = query_api.query(query, org=ORG)

        if tables:
            for table in tables:
                for record in table.records:
                    labels.append(record.values.get("Label", "Unknown"))
                    attacks.append(record.values.get("Attack", "Unknown"))
                    break
        else:
            labels.append("Unknown")
            attacks.append("Unknown")

    df["Label"] = labels
    df["Attack"] = attacks
    df.to_csv(csv_file, index=False)
    print("Ground truth added successfully!")

