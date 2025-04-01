from influxdb_client import InfluxDBClient, Point, WriteOptions
import json

# Sample JSON Data
json_data = '''{
  "timestamp": 1424219007.658518,
  "summary": "CookedLinux / IP / TCP 175.45.176.3:22592 > 149.171.126.16:imap2 PA / Raw",
  "length": 77,
  "layers": [
    {
      "name": "cooked linux",
      "fields": {
        "pkttype": 0,
        "lladdrtype": 1,
        "lladdrlen": 6,
        "src": "021ac50000000000",
        "proto": 2048
      }
    },
    {
      "name": "IP",
      "fields": {
        "version": 4,
        "ihl": 5,
        "tos": 0,
        "len": 61,
        "id": 50301,
        "flags": "",
        "frag": 0,
        "ttl": 63,
        "proto": 6,
        "chksum": 17489,
        "src": "175.45.176.3",
        "dst": "149.171.126.16"
      }
    },
    {
      "name": "TCP",
      "fields": {
        "sport": 22592,
        "dport": 143,
        "seq": 1417884147,
        "ack": 3077387971,
        "dataofs": 5,
        "reserved": 0,
        "flags": "PA",
        "window": 16383,
        "chksum": 45821
      }
    },
    {
      "name": "Raw",
      "fields": {
        "load": "613030332053454c4543542022494e424f58220d0a"
      }
    }
  ]
}'''

# Parse JSON
data = json.loads(json_data)

# Extract Timestamp
timestamp = int(data.get("timestamp", 0) * 1e9)  # Convert to nanoseconds

# Use summary as measurement name (fallback to "network_packet")
measurement = data.get("summary", "network_packet").split(" ")[0]

# Separate Tags and Fields
tags = {}
fields = {}

# Add global fields
fields["length"] = data.get("length", 0)
fields["timestamp"] = data.get("timestamp", 0)  # Store timestamp as a field

# Process Layers Dynamically
for layer in data.get("layers", []):
    layer_name = layer.get("name", "unknown").replace(" ", "_")  # Normalize
    for key, value in layer.get("fields", {}).items():
        if isinstance(value, (int, float)):  # Numeric values → Fields
            fields[f"{layer_name}_{key}"] = value
        else:  # String values → Tags
            tags[f"{layer_name}_{key}"] = str(value)

# Create InfluxDB Point
point = Point(measurement)
for k, v in tags.items():
    point.tag(k, v)
for k, v in fields.items():
    point.field(k, v)
point.time(timestamp)  # Use timestamp as index

print(point)

print("Data written dynamically to InfluxDB successfully!")