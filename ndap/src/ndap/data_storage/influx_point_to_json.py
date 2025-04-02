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
    """Reconstructs JSON packets properly from InfluxDB query results."""
    try:
        if current_packet is None:
            current_packet = {
                "timestamp": record_values.get("timestamp", 0),
                "timestamp_iso": record_values.get("timestamp_iso", ""),
                "summary": record_values.get("pkt_summary", ""),  # Use renamed summary
                "length": record_values.get("length", 0),
                "layers": []
            }

        # Process layers dynamically
        for key, value in record_values.items():
            if key.startswith("layers_"):
                parts = key.split("_")
                layer_index = int(parts[1])  # Extract layer index
                field_type = parts[2]

                # Ensure the layer exists
                while len(current_packet["layers"]) <= layer_index:
                    current_packet["layers"].append({"name": "", "fields": {}})

                # Assign layer name
                if field_type == "name":
                    current_packet["layers"][layer_index]["name"] = value

                # Assign field values inside "fields"
                elif field_type == "fields" and len(parts) > 3:
                    field_name = "_".join(parts[3:])  # Extract actual field name
                    current_packet["layers"][layer_index]["fields"][field_name] = value

        return current_packet

    except Exception as e:
        logging.error(f"Reconstruction failed: {str(e)}")
        logging.error(f"Problematic record: {record_values}")
        return current_packet

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
                        "layers_2_fields_load", "layers_3_name", "layers_3_fields_load"])  // ✅ Added missing "load" field
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
        with open("output.json", "w") as f:
            json.dump(packets, f, indent=2)

    except KeyboardInterrupt:
        logging.info("Shutting down gracefully...")
    finally:
        influx_client.close()

if __name__ == "__main__":
    main()
