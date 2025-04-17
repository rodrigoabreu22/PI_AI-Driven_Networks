import json
from scapy.all import *
from scapy.utils import wrpcap
import sys

def convert_field_value(layer_name, field_name, field_value):
    """Convert field values to the appropriate type for Scapy."""
    if isinstance(field_value, str):
        # Convert to bytes for MAC addresses
        if layer_name.lower() == 'cooked linux' and field_name == 'src':
            if len(field_value) % 2 == 0 and all(c in '0123456789abcdefABCDEF' for c in field_value):
                try:
                    return bytes.fromhex(field_value)
                except:
                    pass

        # Convert all 'load' fields (Raw, Padding, etc.)
        if field_name == 'load':
            if len(field_value) % 2 == 0 and all(c in '0123456789abcdefABCDEF' for c in field_value):
                try:
                    return bytes.fromhex(field_value)
                except:
                    pass

    return field_value

def dict_to_packet(packet_dict):
    """Convert a packet dictionary back to a Scapy packet."""
    layers = []

    for layer in packet_dict['layers']:
        layer_name = layer['name']
        fields = layer['fields']

        processed_fields = {}
        for field_name, field_value in fields.items():
            value = convert_field_value(layer_name, field_name, field_value)

            # Strip problematic auto-generated fields
            if layer_name.upper() in ['IP', 'TCP']:
                if field_name.lower() in ['len', 'chksum', 'dataofs']:
                    continue

            processed_fields[field_name] = value

        # Special-case mappings
        if layer_name.lower() == 'cooked linux':
            layer_cls = CookedLinux
        elif layer_name.lower() == 'padding':
            layer_cls = Padding
        elif layer_name.lower() == 'raw':
            layer_cls = Raw
        else:
            # Try finding Scapy class dynamically
            layer_cls = globals().get(layer_name.upper(), None)
            if layer_cls is None:
                layer_cls = globals().get(layer_name.title().replace(' ', ''), None)

        if layer_cls:
            try:
                layer_obj = layer_cls(**processed_fields)
                layers.append(layer_obj)
            except Exception as e:
                print(f"Warning: Couldn't construct {layer_name} layer normally: {e}", file=sys.stderr)
                try:
                    layer_obj = layer_cls()
                    for f, v in processed_fields.items():
                        if hasattr(layer_obj, f):
                            setattr(layer_obj, f, v)
                    layers.append(layer_obj)
                except Exception as e2:
                    print(f"Error: Failed to reconstruct {layer_name} layer: {e2}", file=sys.stderr)
        else:
            print(f"Warning: Unknown layer type: {layer_name}", file=sys.stderr)

    if not layers:
        raise ValueError("No valid layers found to reconstruct packet")

    packet = layers[0]
    for layer in layers[1:]:
        packet = packet / layer

    # Restore timestamp
    if 'timestamp' in packet_dict:
        packet.time = packet_dict['timestamp']

    return packet

def json_to_pcap(json_file, output_pcap):
    """Convert JSON packets to PCAP file."""
    with open(json_file, 'r') as f:
        try:
            packets_data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error reading JSON file: {e}", file=sys.stderr)
            return False

    if not isinstance(packets_data, list):
        packets_data = [packets_data]

    packets = []
    for pkt_data in packets_data:
        try:
            packet = dict_to_packet(pkt_data)
            packets.append(packet)
        except Exception as e:
            print(f"Error processing packet: {e}", file=sys.stderr)
            continue

    if not packets:
        print("Error: No valid packets found to write", file=sys.stderr)
        return False

    try:
        wrpcap(output_pcap, packets)
        print(f"Successfully wrote {len(packets)} packets to {output_pcap}")
        return True
    except Exception as e:
        print(f"Error writing PCAP file: {e}", file=sys.stderr)
        return False

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python json_to_pcap.py <input.json> <output.pcap>")
        sys.exit(1)

    if not json_to_pcap(sys.argv[1], sys.argv[2]):
        sys.exit(1)
