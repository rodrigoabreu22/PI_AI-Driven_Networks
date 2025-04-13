from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

data = {
    "IPV4_SRC_ADDR": "192.168.1.1",
    "L4_SRC_PORT": 1234,
    "IPV4_DST_ADDR": "192.168.1.2",
    "L4_DST_PORT": 80,
    "PROTOCOL": 6,
    "L7_PROTO": 1.0,
    "IN_BYTES": 1000,
    "IN_PKTS": 10,
    "OUT_BYTES": 800,
    "OUT_PKTS": 8,
    "TCP_FLAGS": 24,
    "CLIENT_TCP_FLAGS": 24,
    "SERVER_TCP_FLAGS": 16,
    "FLOW_DURATION_MILLISECONDS": 0,
    "DURATION_IN": 0,
    "DURATION_OUT": 0,
    "MIN_TTL": 31,
    "MAX_TTL": 32,
    "LONGEST_FLOW_PKT": 100,
    "SHORTEST_FLOW_PKT": 50,
    "MIN_IP_PKT_LEN": 50,
    "MAX_IP_PKT_LEN": 100,
    "SRC_TO_DST_SECOND_BYTES": 456.0,
    "DST_TO_SRC_SECOND_BYTES": 435.0,
    "RETRANSMITTED_IN_BYTES": 0,
    "RETRANSMITTED_IN_PKTS": 0,
    "RETRANSMITTED_OUT_BYTES": 0,
    "RETRANSMITTED_OUT_PKTS": 0,
    "SRC_TO_DST_AVG_THROUGHPUT": 3648000,
    "DST_TO_SRC_AVG_THROUGHPUT": 3480000,
    "NUM_PKTS_UP_TO_128_BYTES": 15,
    "NUM_PKTS_128_TO_256_BYTES": 0,
    "NUM_PKTS_256_TO_512_BYTES": 0,
    "NUM_PKTS_512_TO_1024_BYTES": 0,
    "NUM_PKTS_1024_TO_1514_BYTES": 0,
    "TCP_WIN_MAX_IN": 7240,
    "TCP_WIN_MAX_OUT": 0,
    "ICMP_TYPE": 0,
    "ICMP_IPV4_TYPE": 0,
    "DNS_QUERY_ID": 0,
    "DNS_QUERY_TYPE": 0,
    "DNS_TTL_ANSWER": 331.0,
    "FTP_COMMAND_RET_CODE": 0,
    "Label": 0,
    "Attack": "Benign"
}

producer.send("PROCESSED_NETWORK_DATA", value=data)
producer.flush()
print("Message sent!")
