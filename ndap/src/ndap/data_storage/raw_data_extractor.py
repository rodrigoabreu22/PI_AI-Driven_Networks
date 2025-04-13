import time
from influxdb_client import InfluxDBClient
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from dotenv import load_dotenv
import os
import json
import logging
from datetime import datetime, timezone

load_dotenv()

# Configurations
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")
KAFKA_TOPIC = "DATA_TO_BE_PROCESSED"
KAFKA_BROKER = 'localhost:29092'
CHECK_INTERVAL = 10  
EMPTY_DB_WAIT_TIME = 5  

# Initialize InfluxDB client
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
query_api = client.query_api()

def create_topic(topic_name, broker, num_partitions=1, replication_factor=1):
    """
    Ensure the Kafka topic exists; create it if it does not.
    """
    admin_client = None
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=broker)
        existing_topics = admin_client.list_topics()
        if topic_name in existing_topics:
            logging.info(f"Topic '{topic_name}' already exists.")
        else:
            topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            admin_client.create_topics([topic])
            logging.info(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"Failed to create topic '{topic_name}': {e}")
    finally:
        if admin_client is not None:
            admin_client.close()

# Initialize Kafka producer
producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

# Initialize logging
def initialize_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("logs/influx_extractor.log")],
    )

def load_last_timestamp():
    """Load last processed timestamp from file, or fetch from InfluxDB if missing."""
    try:
        with open("last_timestamp.txt", "r") as f:
            return f.read().strip()
        
    except FileNotFoundError:
        return fetch_earliest_timestamp()

def save_last_timestamp(timestamp):
    """Save the last processed timestamp to a file."""
    with open("last_timestamp.txt", "w") as f:
        f.write(str(timestamp))

def fetch_earliest_timestamp():
    """Retrieve the earliest timestamp in the database or return None if no data exists."""

    query = f"""
        from(bucket: "raw_data")
        |> range(start: 1970-01-01T00:00:00Z)
        |> filter(fn: (r) => r._measurement == "network_packets")
        |> filter(fn: (r) => r._field == "summary")
        |> group(columns: []) 
        |> first()
        """
        #|> keep(columns: ["_time"])
    
    tables = query_api.query(query)
    for table in tables:
        for record in table.records:
            logging_time = record["_time"]

            # Parse the timestamp to a datetime object
            dt = datetime.fromisoformat(str(logging_time))

            # Convert to the format with 'Z' instead of '+00:00'
            converted_timestamp = dt.replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            logging.info(f"last timestamp: {converted_timestamp}")
            return converted_timestamp  # Return the earliest timestamp found
    
    return None  

def main():
    """Main function that continuously checks for new data and pushes it to Kafka."""
    initialize_logging()
    logging.info("Starting InfluxDB data extraction...")
    create_topic(KAFKA_TOPIC, KAFKA_BROKER)

    while True:
        try:
            last_processed_timestamp = load_last_timestamp()

            # If the database is empty, wait and retry
            if last_processed_timestamp is None:
                time.sleep(EMPTY_DB_WAIT_TIME)
                continue  
            
            query = f"""
                from(bucket: "{INFLUXDB_BUCKET}")
                |> range(start: {last_processed_timestamp})
                |> filter(fn: (r) => r._measurement == "network_packets")
                |> rename(columns: {{summary: "pkt_summary"}})
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                |> group(columns: [])          
                |> sort(columns: ["_time"])     
                |> limit(n: 100) 
            """

            optional_fields = {
                "layers_2_fields_options_0_0", "layers_2_fields_options_0_1",
                "layers_2_fields_options_1_0", "layers_2_fields_options_1_1",
                "layers_2_fields_options_2_0", "layers_2_fields_options_2_1"
            }

            ip_irrelevant_fields = {
                "layers_1_fields_ptype", "layers_1_fields_psrc", "layers_1_fields_plen",
                "layers_1_fields_pdst", "layers_1_fields_op", "layers_1_fields_hwtype",
                "layers_1_fields_hwsrc", "layers_1_fields_hwlen", "layers_1_fields_hwdst"
            }

            raw_or_padding_irrelevant_fields = {
                "layers_3_fields_community", "layers_3_fields_pdu",
                "layers_3_fields_version", "layers_3_fields_length",
                "layers_3_fields_reserved", "layers_3_fields_type",
                "layers_3_fields_version"
            }

            irrelevant_layer_3 = {
                "layers_3_name", "layers_3_fields_load",
                "layers_3_fields_community", "layers_3_fields_pdu", "layers_3_fields_version"
            }

            irrelevant_layer_4 = {
                "layers_4_name", "layers_4_fields_load"
            }

            nbt_irrelevant_fields = {
                "layers_3_fields_community", "layers_3_fields_load",
                "layers_3_fields_pdu", "layers_3_fields_version"
            }

            udp_irrelevant_fields = {
                "layers_2_fields_seq", "layers_2_fields_ack", "layers_2_fields_dataofs",
                "layers_2_fields_reserved", "layers_2_fields_flags", "layers_2_fields_window",
                "layers_2_fields_urgptr"
            }

            snmp_irrelevant_fields = {
                "layers_3_fields_load", "layers_3_fields_type",
                "layers_3_fields_length", "layers_3_fields_reserved"
            }

            arp_irrelevant_fields = {
                "layers_1_fields_version", "layers_1_fields_ihl", "layers_1_fields_tod",
                "layers_1_fields_len", "layers_1_fields_id", "layers_1_fields_flags",
                "layers_1_fields_frag", "layers_1_fields_ttl", "layers_1_fields_proto",
                "layers_1_fields_chksum", "layers_1_fields_src", "layers_1_fields_dst"
            }

            smb_irrelevant_fields = {
                "layers_4_fields_load"
            }

            raw_or_padding_irrelevant_fields_layer2 = {
                "layers_2_fields_seq", "layers_2_fields_ack", "layers_2_fields_dataofs",
                "layers_2_fields_reserved", "layers_2_fields_flags", "layers_2_fields_window",
                "layers_2_fields_urgptr", "layers_2_fields_sport", "layers_2_fields_dport", 
                "layers_2_fields_len", "layers_2_fields_chksum"
            }


            tables = query_api.query(query)

            cleaned = []
            for table in tables:
                for record in table.records:
                    values = record.values.copy() 

                    # Remove irrelevant IP-related fields if `layers_1_name` is "IP"
                    if values.get("layers_1_name") == "IP":
                        for field in ip_irrelevant_fields:
                            values.pop(field, None)
                    elif values.get("layers_1_name") == "UDP":
                        for field in udp_irrelevant_fields:
                            values.pop(field, None)
                    elif values.get("layers_1_name") == "ARP":
                        for field in arp_irrelevant_fields:
                            values.pop(field, None)
                    
                    # Remove `layers_2_fields_load` if `layers_2_name` is "TCP" and load is None
                    if values.get("layers_2_name") == "TCP" and values.get("layers_2_fields_load") is None:
                        values.pop("layers_2_fields_load", None)
                        values.pop("layers_2_fields_len", None)
                    
                    elif values.get("layers_2_name") == "UDP":
                        for field in udp_irrelevant_fields:
                            values.pop(field, None)
                    
                    elif values.get("layer_2_name") in ["Padding", "Raw"]:
                        for field in raw_or_padding_irrelevant_fields_layer2:
                            values.pop(field, None)


                    # Remove irrelevant Raw or Padding-related fields if `layers_3_name` is "raw"
                    if values.get("layers_3_name") == "Raw" or values.get("layers_3_name") == "Padding":
                        for field in raw_or_padding_irrelevant_fields:
                            values.pop(field, None)
                    elif values.get("layers_3_name") == "SNMP":
                        for field in snmp_irrelevant_fields:
                            values.pop(field, None)
                    elif values.get("layers_3_name") == "NBT Session Packet":
                        for field in nbt_irrelevant_fields:
                            values.pop(field, None)

                    if values.get("layers_4_name") == "SMB Generic dispatcher":
                        for field in smb_irrelevant_fields:
                            values.pop(field, None)

                    # Remove all optional fields if they are all None
                    if all(values.get(field) is None for field in optional_fields):
                        for field in optional_fields:
                            values.pop(field, None)

                    # Remove layer3 if `layers_3_name` is None
                    if values.get("layers_3_name") == None or values.get("layers_3_name")=="":
                        for field in irrelevant_layer_3:
                            values.pop(field, None)

                    # Remove layer4 if `layers_4_name` is None
                    if values.get("layers_4_name") == None or values.get("layers_4_name")=="":
                        for field in irrelevant_layer_4:
                            values.pop(field, None)

                    cleaned.append(values)

            packets = []

            for record_values in cleaned:
                reconstructed_packet = reconstruct_json(record_values)
                packets.append(reconstructed_packet)

            if packets:
                logging.info(f"packets size: {len(packets)}")

                with open("output.json", "w") as f:
                    json.dump(packets, f, indent=2)
                    f.write("\n")
                
                for packet in packets:
                    logging.info(f"packet: {packet}")
                    producer.send(KAFKA_TOPIC, packet)

                # Update last processed timestamp
                last_packet = packets[-1]
                if "timestamp" in last_packet:
                    last_processed_timestamp = last_packet["timestamp"]
                    
                    dt = datetime.fromtimestamp(last_processed_timestamp, tz=timezone.utc)
                    # Format to ISO 8601 with 'Z'
                    formatted_timestamp = dt.strftime('%Y-%m-%dT%H:%M:%S.%f') + 'Z'
                    
                    save_last_timestamp(formatted_timestamp)

            time.sleep(CHECK_INTERVAL)

        except Exception as e:
            print(f"Error: {e}")
            time.sleep(10)  # Retry after a short delay in case of errors


def reconstruct_json(record_values, current_packet=None):
    """Reconstructs JSON packets properly from InfluxDB query results.
    
    Args:
        record_values: Dictionary containing packet data fields from InfluxDB
        current_packet: Partially reconstructed packet (used for recursive calls)
        
    Returns:
        Dictionary representing the reconstructed packet in JSON format
    """
    try:
        # Initialize packet structure if this is the first call
        if current_packet is None:
            current_packet = {
                "timestamp": record_values.get("timestamp", 0),
                "timestamp_iso": record_values.get("timestamp_iso", ""),
                "summary": record_values.get("pkt_summary", ""),
                "length": record_values.get("length", 0),
                "layers": []
            }

        options_dict = {}  # Temporary storage for options
        
        # Process all fields in the record
        for key, value in record_values.items():
            if not key.startswith("layers_"):
                continue
                
            parts = key.split("_")
            try:
                layer_index = int(parts[1])  # Extract layer index
            except (IndexError, ValueError):
                logging.warning(f"Invalid layer index in key: {key}")
                continue

            # Ensure we have enough layers in the packet
            while len(current_packet["layers"]) <= layer_index:
                current_packet["layers"].append({"name": "", "fields": {}})

            # Skip if layer index is somehow still invalid
            if layer_index >= len(current_packet["layers"]):
                continue

            field_type = parts[2] if len(parts) > 2 else None

            # Handle layer name assignment
            if field_type == "name" and value and value!='':
                current_packet["layers"][layer_index]["name"] = value
                continue

            # Handle regular fields
            if field_type == "fields" and len(parts) > 3:
                # Special handling for TCP/IP options
                if "options" in parts:
                    try:
                        if len(parts) >= 5:  # Need at least layers_X_fields_options_Y_Z
                            option_index = int(parts[4])
                            option_type = int(parts[5]) if len(parts) > 5 else 0
                            
                            if layer_index not in options_dict:
                                options_dict[layer_index] = {}

                        
                            if option_type == 0 and value!= None:
                                 if option_index not in options_dict[layer_index]:
                                     options_dict[layer_index][option_index] = [value, None]
                                 else:
                                     options_dict[layer_index][option_index][0] = value
                        
                            elif option_type == 1 and value != None:
                                if option_index not in options_dict[layer_index]:
                                    options_dict[layer_index][option_index] = ["", value]
                                else: 
                                    options_dict[layer_index][option_index][1] = value
                    
                    except (IndexError, ValueError) as e:
                        logging.warning(f"Malformed option field {key}: {e}")
                    continue
                
                # Handle all other fields
                field_name = "_".join(parts[3:])  # Reconstruct original field name
                if value != None:
                    current_packet["layers"][layer_index]["fields"][field_name] = value
                elif "flags" in field_name:
                    current_packet["layers"][layer_index]["fields"][field_name] = None

        # Process collected options
        for layer_index, options in options_dict.items():
            if layer_index < len(current_packet["layers"]):
                sorted_options = sorted(options.items())
                options_list = [[opt_name, opt_value] for _, (opt_name, opt_value) in sorted_options]
                current_packet["layers"][layer_index]["fields"]["options"] = options_list


        for layer in current_packet["layers"]:

            if "flags" in layer["fields"] and layer["fields"]["flags"] is None:
                if layer["name"] in ["IP", "TCP"]:
                    layer["fields"]["flags"] = ""
                else: 
                    del layer["fields"]["flags"]
                
            if layer["name"] in ["IP", "TCP"] and "options" not in layer["fields"]:
                layer["fields"]["options"] = []


            if layer["name"] == "NBT Session Packet":
                layer["fields"] = {k.upper(): v for k, v in layer["fields"].items()}


        current_packet["layers"] = [
            layer for layer in current_packet["layers"]
            if layer.get("name") or layer.get("fields")
        ]

        return current_packet
    
    except Exception as e:
        logging.error(f"Reconstruction failed with error: {e!r}", exc_info=True)
        logging.error(f"Problematic record: {record_values}")
        return current_packet or {}  # Return empty dict if current_packet is None

if __name__ == "__main__":
    main()