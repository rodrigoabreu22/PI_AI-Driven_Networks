import json
import logging
import os
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient

# Load environment variables
load_dotenv()

INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")

# Initialize logging
def initialize_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("logs/influx_extractor.log"), logging.StreamHandler()],
    )

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
            if field_type == "name":
                current_packet["layers"][layer_index]["name"] = value or ""
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
                            
                            if option_type == 0 and value:
                                if option_index not in options_dict[layer_index]:
                                    options_dict[layer_index][option_index] = [value, None]
                                else:
                                    options_dict[layer_index][option_index][0] = value
                        
                            elif option_type == 1:
                                if option_index not in options_dict[layer_index]:
                                    options_dict[layer_index][option_index] = ["", value]
                                else: 
                                    options_dict[layer_index][option_index][1] = value
                    except (IndexError, ValueError) as e:
                        logging.warning(f"Malformed option field {key}: {e}")
                    continue
                
                # Handle all other fields
                field_name = "_".join(parts[3:])  # Reconstruct original field name
                current_packet["layers"][layer_index]["fields"][field_name] = value

        # Process collected options
        for layer_index, options in options_dict.items():
            if layer_index < len(current_packet["layers"]):
                sorted_options = sorted(options.items())
                options_list = [[opt_name, opt_value] for _, (opt_name, opt_value) in sorted_options]
                current_packet["layers"][layer_index]["fields"]["options"] = options_list

        # Ensure "options" exists for all relevant layers
        first_layer = True
        for layer in current_packet["layers"]:
            if layer["name"] in ["", "Raw", "Padding"] or first_layer:
                first_layer=False
                continue
                
            if "options" not in layer["fields"]:
                layer["fields"]["options"] = []

            # Special handling for IP flags
            if layer["name"] == "IP" and "flags" in layer["fields"]:
                if layer["fields"]["flags"] is None:
                    layer["fields"]["flags"] = ""

        return current_packet

    except Exception as e:
        logging.error(f"Reconstruction failed with error: {e!r}", exc_info=True)
        logging.error(f"Problematic record: {record_values}")
        return current_packet or {}  # Return empty dict if current_packet is None


def query_influx_data(query_api, limit=50):
    """Query packet data from InfluxDB with proper field extraction"""
    query = f"""
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: 2015-02-18T00:23:25Z, stop: 2015-02-18T00:23:40Z)
      |> filter(fn: (r) => r._measurement == "network_packets")
      |> rename(columns: {{summary: "pkt_summary"}})  // Avoid name conflicts
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> keep(columns: ["_time", "pkt_summary", "length", "timestamp", "timestamp_iso",
                        "layers_0_name", "layers_0_fields_pkttype", "layers_0_fields_proto",
                        "layers_0_fields_lladdrtype", "layers_0_fields_lladdrlen", "layers_0_fields_src",
                        "layers_1_name", "layers_1_fields_version", "layers_1_fields_ihl", "layers_1_fields_tos",
                        "layers_1_fields_len", "layers_1_fields_id", "layers_1_fields_flags", "layers_1_fields_frag",
                        "layers_1_fields_ttl", "layers_1_fields_proto", "layers_1_fields_chksum",
                        "layers_1_fields_src", "layers_1_fields_dst", "layers_1_fields_options",
                        "layers_2_name", "layers_2_fields_sport", "layers_2_fields_dport",
                        "layers_2_fields_seq", "layers_2_fields_ack", "layers_2_fields_dataofs",
                        "layers_2_fields_reserved", "layers_2_fields_flags", "layers_2_fields_window",
                        "layers_2_fields_chksum", "layers_2_fields_urgptr", "layers_2_fields_options",
                        "layers_2_fields_options_0_0", "layers_2_fields_options_0_1", "layers_2_fields_options_1_0", 
                        "layers_2_fields_options_1_1", "layers_2_fields_options_2_0", "layers_2_fields_options_2_1", 
                        "layers_2_fields_load", "layers_3_name", "layers_3_fields_load"])
      |> limit(n: {limit})
    """

    try:
        result = query_api.query(query)
        packets = []

        for table in result:
            for record in table.records:
                record_values = record.values
                reconstructed_packet = reconstruct_json(record_values)
                packets.append(reconstructed_packet)

        return packets

    except Exception as e:
        logging.error(f"Error querying InfluxDB: {str(e)}")
        return []

def main():
    initialize_logging()
    logging.info("Starting InfluxDB data extraction...")

    # Initialize InfluxDB client
    influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    query_api = influx_client.query_api()

    try:
        packets = query_influx_data(query_api)
        logging.info(f"Successfully extracted {len(packets)} packets")

        # Save JSON output
        with open("output2.json", "w") as f:
            json.dump(packets, f, indent=2)

    except KeyboardInterrupt:
        logging.info("Shutting down gracefully...")
    finally:
        influx_client.close()

if __name__ == "__main__":
    main()
